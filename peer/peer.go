package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"anraft/utils"
	"fmt"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"time"
)

/************************************************************************************
                        +---------------------------------------------------------+
                        |                            RoleMaintain Loop            |
                        |                                                         |
                        |                          +--------------------------+   |
                        |                          |   Leader  Loop           |   |
                        |                    +-----|                          |   |
                        |                    |     +--------------------------+   |
     +-----------+      |      +----------+  |     +--------------------------+   |
     | Rpc       |-------------| Pipe Pair|--|-----|   Follower Loop          |   |
     +-----------+      |      +----------+  |     |                          |   |
                        |                    |     +--------------------------+   |
                        |                    |     +--------------------------+   |
                        |                    +-----|   Candidate Loop         |   |
                        |                          |                          |   |
                        |                          +--------------------------+   |
                        |                                                         |
                        +---------------------------------------------------------+

NOTE(deyukong): the thread model is above, meta changing is processed only in
RoleMaintain loop(thread), a rpc(like appendentry) from other threads may require
synchronous process. It passes params through new_entry_pair.input and wait on
new_entry_pair.output. Three sub-loops in RoleMaintain loop transfers between each
other with roles. but they are in one thread, so new_entry_pair is handled without
race problems.
***********************************************************************************/

// appendEntry result
type AeResult int

const (
	AE_OK AeResult = iota
	AE_RETRY
	AE_SMALL_TERM
	AE_TERM_UNMATCH
)

const (
	LAG_MAX = 1000
)

type PeerInfo struct {
	host   string
	id     string
	client pb.PeerClient
}

type NewEntryPair struct {
	input  chan *pb.AppendEntriesReq
	output chan *pb.AppendEntriesRes
}

type VotePair struct {
	input  chan *pb.RequestVoteReq
	output chan *pb.RequestVoteRes
}

func (p *PeerInfo) Init(id string, host string) error {
	// NOTE(deyukong): things may be difficult if servers cheat about their ids.
	// but raft is non-byzantine, it's ops' duty to ensure this.
	p.host = host
	p.id = id
	conn, err := grpc.Dial(p.host,
		grpc.WithInsecure(),
		// NOTE(deyukong): no need to block, grpc-client auto reconnects
		// grpc.WithBlock(),
		// grpc.WithTimeout(timeout))
	)
	if err != nil {
		return err
	}
	p.client = pb.NewPeerClient(conn)
	return nil
}

type PeerServer struct {
	host         string
	id           string
	cluster_info []*PeerInfo
	mutex        sync.Mutex
	// in-mem cache of the in-disk persistent term, protected by mutex
	current_term int64
	// in-mem cache of the in-disk persistent vote_for, protected by mutex
	vote_for string
	// in-mem cache of the persist commit-index
	commit_index   int64
	state          pb.PeerState
	store          *PeerStorage
	new_entry_pair *NewEntryPair
	vote_pair      *VotePair
	close_chan     chan int
	/* the name of election_timeout is from 5.2 */
	/* if a follower receives on communication over a period of time */
	/* called the election timeout, then ...... */
	election_timeout  time.Duration
	election_interval time.Duration
}

func (p *PeerServer) Init(my_id, my_host string, hosts map[string]string,
	store_engine storage.Storage, etime time.Duration) error {
	if _, ok := hosts[my_host]; !ok {
		return fmt.Errorf("self should be in cluster")
	}
	p.host = my_host
	p.id = my_id
	p.cluster_info = []*PeerInfo{}
	for id, host := range hosts {
		peerInfo := PeerInfo{}
		peerInfo.Init(id, host)
		p.cluster_info = append(p.cluster_info, &peerInfo)
	}
	p.election_timeout = etime
	p.election_interval = etime / 3
	/* see 5.2 leader election */
	/* when servers start up, they begin as followers */
	p.state = pb.PeerState_Follower
	p.new_entry_pair = &NewEntryPair{
		input:  make(chan *pb.AppendEntriesReq),
		output: make(chan *pb.AppendEntriesRes),
	}
	p.vote_pair = &VotePair{
		input:  make(chan *pb.RequestVoteReq),
		output: make(chan *pb.RequestVoteRes),
	}
	p.close_chan = make(chan int)
	p.store = &PeerStorage{}
	p.store.Init(store_engine)

	var err error = nil
	if p.current_term, err = p.store.GetTerm(); err != nil {
		return err
	}
	if p.vote_for, err = p.store.GetVoteFor(); err != nil {
		return err
	}
	if p.commit_index, err = p.store.GetCommitIndex(); err != nil {
		return err
	}
	go p.RoleManageThd()
	return nil
}

func (p *PeerServer) HeartBeat(context.Context, *pb.HeartBeatReq) (*pb.HeartBeatRes, error) {
	return nil, nil
}

func (p *PeerServer) ElecTimeout() {
	if p.state != pb.PeerState_Follower {
		log.Fatalf("ElecTimeout in wrong state:%v", p.state)
	}
	p.Elect()
}

// NOTE(deyukong): to make the thread model simple, we only allow RoleManageThd thread to change
// peer's state, other threads communicate with this thread
func (p *PeerServer) RoleManageThd() {
	for {
		if p.state == pb.PeerState_Follower {
			p.FollowerCron()
		} else if p.state == pb.PeerState_Leader {
			p.LeaderCron()
		} else if p.state == pb.PeerState_Candidate {
			log.Fatalf("candidate state, code should not reach here")
		} else {
			log.Fatalf("invalid state:%d", int(p.state))
		}
	}
}

func (p *PeerServer) appendNewEntries(entries *pb.AppendEntriesReq) *pb.AppendEntriesRes {
	// it's the upper logic's duty to guarantee the correct role at this point
	if p.state != pb.PeerState_Follower {
		log.Fatalf("append entry but role[%v] not follower", p.state)
	}
	if p.current_term > entries.Term {
		log.Infof("append entry but smaller term[%d:%d]", p.current_term, entries.Term)
		return &pb.AppendEntriesRes{
			Header: new(pb.ResHeader),
			Term:   p.current_term,
			Result: int32(AE_SMALL_TERM),
		}
	}

	p.UpdateTerm(entries.Term)

	var iter storage.Iterator = nil
	if entries.PrevLogIndex == -1 {
		// NOTE(deyukong): currently, committed logs are not archieved, so seek from 0
		iter = p.store.SeekLogAt(0)
		defer iter.Close()
	} else {
		iter = p.store.SeekLogAt(entries.PrevLogIndex)
		defer iter.Close()

		// seek to the end
		if !iter.ValidForPrefix(LOG_PREFIX) {
			return &pb.AppendEntriesRes{
				Header: new(pb.ResHeader),
				Term:   entries.Term,
				Result: int32(AE_TERM_UNMATCH),
			}
		}
		entry, err := IterEntry2Log(iter)
		if err != nil {
			log.Fatalf("idx:%d parse rawlog to logentry failed:%v", entries.PrevLogIndex, err)
		}
		// entry not match
		if entry.Term != entries.PrevLogTerm {
			log.Warnf("master[%s] entry:[%d:%d] not match mine[%d:%d]",
				entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm, entry.Index, entry.Term)
			return &pb.AppendEntriesRes{
				Header: new(pb.ResHeader),
				Term:   entries.Term,
				Result: int32(AE_TERM_UNMATCH),
			}
		}
		// prev_entry matches, move iter to the next
		iter.Next()
	}

	// at most times, code reaches here with iter invalid
	if iter.ValidForPrefix(LOG_PREFIX) {
		log.Warnf("master[%s] replays old log:[%d:%d]",
			entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm)
	}

	// set apply_from default to len(entries.Entries), if all matches, we have nothing to apply
	apply_from := len(entries.Entries)
	for idx, master_entry := range entries.Entries {
		// seek to the end
		if !iter.ValidForPrefix(LOG_PREFIX) {
			apply_from = idx
			break
		}
		entry, err := IterEntry2Log(iter)
		if err != nil {
			// NOTE(deyukong): master_entry.Index is not mine, but it should be the same,
			// just for problem-tracking
			log.Fatalf("idx:%d parse rawlog to logentry failed:%v", master_entry.Index, err)
		}
		if entry.Index != master_entry.Index {
			log.Fatalf("bug:master[%s] entry:[%d:%d] index defers from  mine[%d:%d]",
				entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm, entry.Index, entry.Term)
		}
		if entry.Term != master_entry.Term {
			apply_from = idx
			break
		}
		// entry.Term == master_entry.Term
		iter.Next()
	}

	// at most times, code reaches here with iter invalid
	for ; iter.ValidForPrefix(LOG_PREFIX); iter.Next() {
		entry, err := IterEntry2Log(iter)
		if err != nil {
			log.Fatalf("logentry parse failed:%v", err)
		}
		if err := p.store.DelLogEntry(entry); err != nil {
			log.Fatalf("del logentry:%v failed:%v", entry, err)
		} else {
			log.Infof("logentry:%v remove succ", entry)
		}
	}

	for i := apply_from; i < len(entries.Entries); i++ {
		if err := p.store.AppendLogEntry(entries.Entries[i]); err != nil {
			log.Fatalf("apply entry:%v to store failed:%v", entries.Entries[i], err)
		} else {
			log.Infof("apply entry:%v to store success", entries.Entries[i])
		}
	}

	// update my commit index
	if p.commit_index > entries.LeaderCommit {
		log.Fatalf("my commitidx:%d shouldnt be greater than master's:%d", p.commit_index, entries.LeaderCommit)
	}

	p.UpdateCommitIndex(utils.Int64Min(entries.LeaderCommit, entries.Entries[len(entries.Entries)-1].Index))

	return &pb.AppendEntriesRes{
		Header: new(pb.ResHeader),
		Term:   entries.Term,
		Result: int32(AE_OK),
	}
}

func (p *PeerServer) LeaderCron() {
	for {
		select {
		case <-p.close_chan:
			log.Infof("leader cron stops...")
			return
		case tmp := <-p.new_entry_pair.input:
			if p.current_term > tmp.Term {
				log.Infof("leader term:%d got entry with smaller term:%d", p.current_term, tmp.Term)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_SMALL_TERM),
					Msg:    fmt.Sprintf("req term:%d smaller than candidate:%d", tmp.Term, p.current_term),
					Term:   p.current_term,
				}
			} else if p.current_term == tmp.Term {
				log.Fatalf("BUG:leader term:%d meet appendentry:%v same term", p.current_term, tmp)
			} else {
				log.Infof("leader term:%d got entry monotonic term:%d, turn follower", p.current_term, tmp.Term)
				p.UpdateTerm(tmp.Term)
				p.ChangeState(pb.PeerState_Follower)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_RETRY),
					Msg:    "",
					Term:   p.current_term,
				}
				return
			}
		case tmp := <-p.vote_pair.input:
			if ok, reason := p.GrantVote(tmp); !ok {
				log.Infof("leader term:%d got vote, not grant:%s", p.current_term, reason)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					// although vote not granted, it is still guaranteed that if other's term is greater,
					// ours is also updated to other's
					Term: p.current_term,
				}
			} else {
				log.Infof("leader term:%d got vote and granted", p.current_term)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					Term:        p.current_term,
				}
				return
			}
		}
	}
}

func (p *PeerServer) FollowerCron() {
	tchan := time.After(p.election_timeout)
	for {
		select {
		case <-p.close_chan:
			log.Infof("follower cron stops...")
			return
		case <-tchan:
			p.ElecTimeout()
			return
		case tmp := <-p.new_entry_pair.input:
			p.new_entry_pair.output <- p.appendNewEntries(tmp)
			tchan = time.After(p.election_timeout)
		case tmp := <-p.vote_pair.input:
			if ok, reason := p.GrantVote(tmp); !ok {
				log.Infof("follower got vote, now_term:%d not grant:%s", p.current_term, reason)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					// although vote not granted, it is still guaranteed that if other's term is greater,
					// ours is also updated to other's
					Term: p.current_term,
				}
			} else {
				log.Infof("follower got vote and granted, current_term:%d", p.current_term)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					Term:        p.current_term,
				}
			}
		}
	}
}

// NOTE(deyukong): this is run in a temp thread, never touch shared states
func request_vote(p *PeerInfo, term int64, timeout time.Duration,
	last_entry *pb.LogEntry, channel chan *pb.RequestVoteRes, wg *sync.WaitGroup) {
	defer wg.Done()
	req := new(pb.RequestVoteReq)
	req.Header = new(pb.ReqHeader)
	req.Term = term
	req.CandidateId = p.id
	req.LastLogIndex = -1
	req.LastLogTerm = -1
	if last_entry != nil {
		req.LastLogIndex = last_entry.Index
		req.LastLogTerm = last_entry.Term
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	rsp, err := p.client.RequestVote(ctx, req)
	if err != nil {
		log.Errorf("request vote:%v failed:%v", req, err)
		return
	}
	channel <- rsp
}

// requires mutex held
func (p *PeerServer) updateTermInLock(new_term int64) error {
	if p.current_term >= new_term {
		log.Fatalf("updateTermInLock with new:%d not greater than mine:%d", new_term, p.current_term)
	}
	p.current_term = new_term
	if err := p.store.SaveTerm(p.current_term); err != nil {
		return err
	}
	p.vote_for = ""
	if err := p.store.SaveVoteFor(p.vote_for); err != nil {
		return err
	}
	return nil
}

// requires mutex held
func (p *PeerServer) voteInLock(id string) error {
	if p.vote_for != "" {
		log.Fatalf("vote_for should be empty")
	}
	p.vote_for = id
	if err := p.store.SaveVoteFor(p.vote_for); err != nil {
		return err
	}
	return nil
}

func (p *PeerServer) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
	now_term, vote_for := p.GetTermAndVoteFor()
	rsp := new(pb.RequestVoteRes)
	rsp.Header = new(pb.ResHeader)
	if req.Term < now_term {
		rsp.Term = now_term
		rsp.VoteGranted = vote_for
		return rsp, nil
	}
	p.vote_pair.input <- req
	return <-p.vote_pair.output, nil
}

// TODO(deyukong): unittests about term, logterm, it's quite complex
func (p *PeerServer) IsLogUpToDate(req *pb.RequestVoteReq) bool {
	last_entry, err := p.store.GetLastLogEntry()
	if err != nil {
		log.Fatalf("get last entry failed:%v", err)
	}
	// mine is nil, whatever is uptodate with me.
	if last_entry == nil {
		return true
	}
	// chapter5.4.2 defines strictly what is "up-to-date"
	// if the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// if the logs end with the same term, then whichever log is longer(NOTE:deyukong, "longer" here I think
	// is synonymous with "greater index') is more up-to-update
	return req.LastLogTerm > last_entry.Term ||
		(req.LastLogTerm == last_entry.Term && req.LastLogIndex >= last_entry.Index)
}

func (p *PeerServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
	now_term := p.GetTerm()
	rsp := new(pb.AppendEntriesRes)
	rsp.Header = new(pb.ResHeader)
	if req.Term < now_term {
		rsp.Term = now_term
		rsp.Result = int32(AE_OK)
		return rsp, nil
	}
	p.new_entry_pair.input <- req
	pipe_output := <-p.new_entry_pair.output
	if pipe_output.Result == int32(AE_RETRY) {
		log.Infof("appendEntry with term:%d retries", req.Term)
		return p.AppendEntries(ctx, req)
	}
	return pipe_output, nil
}

func (p *PeerServer) UpdateCommitIndex(idx int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// it's upper logic's duty to guarentee this
	if p.commit_index > idx {
		log.Fatalf("UpdateCommitIndex with smaller index:%d, mine:%d", idx, p.commit_index)
	}
	if p.commit_index == idx {
		return
	}

	if err := p.store.SaveCommitIndex(idx); err != nil {
		log.Fatalf("SaveCommitIndex:%d failed:%v", idx, err)
	}
	p.commit_index = idx
}

func (p *PeerServer) VoteForSelf(channel chan *pb.RequestVoteRes) int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if err := p.updateTermInLock(p.current_term + 1); err != nil {
		log.Fatalf("incrTermInLock failed:%v", err)
	}
	if err := p.voteInLock(p.id); err != nil {
		log.Fatalf("voteInLock failed:%v", err)
	}
	rsp := new(pb.RequestVoteRes)
	rsp.Header = new(pb.ResHeader)
	rsp.Term = p.current_term
	rsp.VoteGranted = p.id
	channel <- rsp
	return p.current_term
}

// NOTE(deyukong): can only be called from main thread
func (p *PeerServer) GrantVote(req *pb.RequestVoteReq) (bool, string) {
	if p.current_term > req.Term {
		return false, fmt.Sprintf("smaller term, mine:%d, his:%d", p.current_term, req.Term)
	}
	if p.current_term == req.Term && p.vote_for != "" && p.vote_for != req.CandidateId {
		return false, fmt.Sprintf("same term:%d voted for others:%s", p.current_term, p.vote_for)
	}

	// here, current_term <= req.Term, no matter vote granted or not, we must update our term
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.current_term < req.Term {
		if err := p.updateTermInLock(req.Term); err != nil {
			log.Fatalf("updateTermInLock failed:%v", err)
		}
	}
	if !p.IsLogUpToDate(req) {
		return false, fmt.Sprintf("log not as uptodate")
	}
	if err := p.voteInLock(req.CandidateId); err != nil {
		log.Fatalf("voteInLock failed:%v", err)
	}
	return true, ""
}

func (p *PeerServer) UpdateTerm(term int64) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.current_term > term {
		log.Fatalf("it's upper logic to guarentee current_term > term before update")
	}
	if p.current_term == term {
		return false
	}
	if err := p.updateTermInLock(term); err != nil {
		log.Fatalf("updateTermInLock failed:%v", err)
	}
	return true
}

// used for user-interactive thread, protected by mutex
func (p *PeerServer) GetTerm() int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.current_term
}

func (p *PeerServer) GetTermAndVoteFor() (int64, string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.current_term, p.vote_for
}

func (p *PeerServer) changeStateInLock(s pb.PeerState) {
	p.state = s
}

func (p *PeerServer) getStateInLock() pb.PeerState {
	return p.state
}

func (p *PeerServer) GetState() pb.PeerState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.getStateInLock()
}

func (p *PeerServer) ChangeState(s pb.PeerState) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.changeStateInLock(s)
}

func (p *PeerServer) Elect() {
	// TODO(deyukong): the rule to modify state
	p.ChangeState(pb.PeerState_Candidate)
	vote_chan := make(chan *pb.RequestVoteRes, len(p.cluster_info))

	new_term := p.VoteForSelf(vote_chan)
	log.Infof("begin a new election with new_term:%d", new_term)
	var wg sync.WaitGroup
	wchan := make(chan int)
	last_entry, err := p.store.GetLastLogEntry()
	if err != nil {
		log.Fatalf("get last entry failed:%v", err)
	}
	for _, o := range p.cluster_info {
		if o.host == p.host {
			continue
		}
		wg.Add(1)
		go request_vote(o, new_term, p.election_timeout, last_entry, vote_chan, &wg)
	}
	go func() {
		wg.Wait()
		close(wchan)
	}()

	for {
		select {
		case <-wchan:
			log.Infof("elect term:%d collect vote done", new_term)
			goto WAIT_ELECT_FOR_END
		case tmp := <-p.new_entry_pair.input:
			if new_term > tmp.Term {
				log.Infof("elect term:%d got entry with smaller term:%d, keep wait", new_term, tmp.Term)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_SMALL_TERM),
					Msg:    fmt.Sprintf("reqterm:%d smaller than candidate:%d", tmp.Term, new_term),
					Term:   new_term,
				}
			} else {
				log.Infof("elect term:%d got entry monotonic term:%d, turn to follower", new_term, tmp.Term)
				p.UpdateTerm(tmp.Term)
				p.ChangeState(pb.PeerState_Follower)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_RETRY),
					Msg:    "",
					Term:   p.current_term,
				}
				return
			}
		case tmp := <-p.vote_pair.input:
			if ok, reason := p.GrantVote(tmp); !ok {
				log.Infof("elect term:%d got vote, now_term:%d not grant:%s", new_term, p.current_term, reason)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					// although vote not granted, it is still guaranteed that if other's term is greater,
					// ours is also updated to other's
					Term: p.current_term,
				}
			} else {
				log.Infof("elect term:%d got vote and granted, current_term:%d", new_term, p.current_term)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					Term:        p.current_term,
				}
				return
			}
		case <-p.close_chan:
			return
		}
	}

WAIT_ELECT_FOR_END:

	max_term := new_term
	same_term_map := make(map[string]int)
	for o := range vote_chan {
		if o.Term < new_term {
			log.Fatalf("RequestVote rep see term:%d less than mine:%d", o.Term, new_term)
		}
		if o.Term > max_term {
			max_term = o.Term
		}
		if o.Term == new_term {
			if _, ok := same_term_map[o.VoteGranted]; ok {
				same_term_map[o.VoteGranted] += 1
			} else {
				same_term_map[o.VoteGranted] = 1
			}
		}
	}
	// see paper chaptor-5, rules for all servers
	// if rpc request or response contains term T > currentTerm,
	// update currentTerm to T and conver to follower
	if max_term > new_term {
		p.UpdateTerm(max_term)
		p.state = pb.PeerState_Follower
		return
	}

	majority_id := ""
	for id, num := range same_term_map {
		if num >= len(p.cluster_info)/2+1 {
			if majority_id == "" {
				majority_id = id
			} else {
				log.Fatalf("term:%d have two ids:%s,%s got major votes", new_term, majority_id, id)
			}
		}
	}
	if majority_id == "" {
		// [0, 2) * p.election_timeout
		nanocount := int64(p.election_timeout / time.Nanosecond)
		sleep := time.Duration(int64(2*rand.Float64()*float64(nanocount))) * time.Nanosecond
		log.Infof("elect draw, sleep for:%v and retry", sleep)
		tchan := time.After(sleep)
		for {
			select {
			case <-tchan:
				p.Elect()
				return
			case tmp := <-p.new_entry_pair.input:
				if new_term > tmp.Term {
					log.Infof("elect term:%d got entry with smaller term:%d, keep wait", new_term, tmp.Term)
					p.new_entry_pair.output <- &pb.AppendEntriesRes{
						Header: new(pb.ResHeader),
						Result: int32(AE_SMALL_TERM),
						Msg:    fmt.Sprintf("reqterm:%d smaller than candidate:%d", tmp.Term, new_term),
						Term:   new_term,
					}
				} else {
					log.Infof("elect term:%d got entry with monotonic term:%d, turn follower", new_term, tmp.Term)
					p.UpdateTerm(tmp.Term)
					p.ChangeState(pb.PeerState_Follower)
					p.new_entry_pair.output <- &pb.AppendEntriesRes{
						Header: new(pb.ResHeader),
						Result: int32(AE_RETRY),
						Msg:    "",
						Term:   p.current_term,
					}
					return
				}
			case tmp := <-p.vote_pair.input:
				if ok, reason := p.GrantVote(tmp); !ok {
					log.Infof("elect term:%d got vote, now_term:%d not grant:%s", new_term, p.current_term, reason)
					p.vote_pair.output <- &pb.RequestVoteRes{
						Header:      new(pb.ResHeader),
						VoteGranted: p.vote_for,
						// although vote not granted, it is still guaranteed that if other's term is greater,
						// ours is also updated to other's
						Term: p.current_term,
					}
				} else {
					log.Infof("elect term:%d got vote and granted, current_term:%d", new_term, p.current_term)
					p.vote_pair.output <- &pb.RequestVoteRes{
						Header:      new(pb.ResHeader),
						VoteGranted: p.vote_for,
						Term:        p.current_term,
					}
					return
				}
			case <-p.close_chan:
				return
			}
		}
	} else if majority_id == p.id {
		p.ChangeState(pb.PeerState_Leader)
	} else {
		p.ChangeState(pb.PeerState_Follower)
	}
}
