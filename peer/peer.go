package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"fmt"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
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

type AeWrapper struct {
	id        string
	res       *pb.AppendEntriesRes
	idx int64
    term int64
}

type IndexAndTerm struct {
    idx int64
    term int64
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
