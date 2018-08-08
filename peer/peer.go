package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"anraft/utils"
	"fmt"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"strings"
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

type LeaderMode int

const (
	LM_NO_CRON LeaderMode = 1 << iota
	LM_HB                 // leader mode heartbeat
	LM_TL                 // leader mode translog
)

func (l LeaderMode) IsHB() bool {
	return (l & LM_HB) != 0
}

func (l LeaderMode) IsTL() bool {
	return (l & LM_TL) != 0
}

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
	id  string
	res *pb.AppendEntriesRes
	it  *IndexAndTerm
}

type IndexAndTerm struct {
	idx  int64
	term int64
}

func (it IndexAndTerm) String() string {
	return fmt.Sprintf("{idx:%d,term:%d}", it.idx, it.term)
}

type IndexAndTerms []*IndexAndTerm

func (its IndexAndTerms) String() string {
	s := []string{}
	for _, o := range its {
		s = append(s, fmt.Sprintf("%v", o))
	}
	return strings.Join(s, ",")
}

func (its IndexAndTerms) Len() int {
	return len(its)
}

func (its IndexAndTerms) Less(i, j int) bool {
	return its[i].idx < its[j].idx
}

func (its IndexAndTerms) Swap(i, j int) {
	its[i], its[j] = its[j], its[i]
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
	id           string
	cluster_info []*PeerInfo
	mutex        sync.RWMutex
	commit_cond  *sync.Cond
	// in-mem cache of the in-disk persistent term, protected by mutex
	current_term int64
	// in-mem cache of the in-disk persistent vote_for, protected by mutex
	vote_for string
	// in-mem cache of the persist commit-index
	commit_index int64
	// current state, protected by mutex
	state          pb.PeerState
	store          *PeerStorage
	new_entry_pair *NewEntryPair
	vote_pair      *VotePair
	closer         *utils.Closer
	/* the name of election_timeout is from 5.2 */
	/* if a follower receives on communication over a period of time */
	/* called the election timeout, then ...... */
	election_timeout  time.Duration
	election_interval time.Duration
	volatile_leader   string
	statm             *StateMachine
}

func (p *PeerServer) Init(my_id string, hosts map[string]string,
	store_engine storage.Storage, etime time.Duration) error {
	if _, ok := hosts[my_id]; !ok {
		return fmt.Errorf("self should be in cluster")
	}
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
	p.closer = utils.NewCloser()
	p.commit_cond = sync.NewCond(&p.mutex)

	p.store = &PeerStorage{}
	p.store.Init(store_engine)

	p.statm = &StateMachine{}
	p.statm.Init(p, p.store, p.closer)

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
	log.Infof("peer:%s start with current_term:%d, vote_for:%s, commit_index:%d",
		my_id, p.current_term, p.vote_for, p.commit_index)
	return nil
}

func (p *PeerServer) Start() {
	p.closer.AddOne()
	go p.RoleManageThd()

	p.statm.Run()
}

// NOTE(deyukong): to make the thread model simple, we only allow RoleManageThd thread to change
// peer's state, other threads communicate with this thread
func (p *PeerServer) RoleManageThd() {
	defer p.closer.Done()
	for {
		select {
		case <-p.closer.HasBeenClosed():
			log.Infof("process shutting down...")
			return
		default: // nothing
		}
		if p.state == pb.PeerState_Follower {
			p.FollowerCron()
		} else if p.state == pb.PeerState_Leader {
			p.LeaderCron(LM_HB | LM_TL)
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

func (p *PeerServer) GetCommitIndex() int64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.commit_index
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
	p.commit_cond.Broadcast()
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
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.current_term
}

func (p *PeerServer) GetTermAndVoteFor() (int64, string) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.current_term, p.vote_for
}

func (p *PeerServer) changeStateInLock(s pb.PeerState) {
	p.state = s
}

func (p *PeerServer) getStateInLock() pb.PeerState {
	return p.state
}

func (p *PeerServer) GetState() pb.PeerState {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.getStateInLock()
}

func (p *PeerServer) WaitCommitApply(idx int64, term int64) error {
	p.statm.WaitCommitApply(idx)

	// NOTE(deyukong): it may not be so precise to compare current_term and my want term
	// but it wont harm correctness. we dont want to do an IO to get idx's logentry in a mutex
	// if p.current_term != term, leader-change may happens during a write and upper-level
	// can do a retry.
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.current_term != term {
		return fmt.Errorf("leader changes during wait[%d,%d]", p.current_term, term)
	}
	return nil
}

func (p *PeerServer) WaitCommit(idx int64, term int64) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for p.commit_index < idx {
		p.commit_cond.Wait()
	}
	// NOTE(deyukong): it may not be so precise to compare current_term and my want term
	// but it wont harm correctness. we dont want to do an IO to get idx's logentry in a mutex
	// if p.current_term != term, leader-change may happens during a write and upper-level
	// can do a retry.
	if p.current_term != term {
		return fmt.Errorf("leader changes during wait[%d,%d]", p.current_term, term)
	}
	return nil
}

func (p *PeerServer) ChangeState(s pb.PeerState) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.changeStateInLock(s)
}

// we must guarentee all threads exits before this function returns
func (p *PeerServer) Stop() {
	log.Infof("server stops")
	p.closer.SignalAndWait()
	log.Infof("server stops done")
}
