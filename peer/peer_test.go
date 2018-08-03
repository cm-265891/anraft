package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage/badger"
	"anraft/utils"
	"fmt"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"os"
	"sync"
	"testing"
	"time"
)

type VoteFunctor func(context.Context, *pb.RequestVoteReq) (*pb.RequestVoteRes, error)
type AeFunctor func(context.Context, *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error)

type MockClient struct {
	pb.PeerClient
	mutex        sync.Mutex
	vote_functor VoteFunctor
	ae_functor   AeFunctor
}

func (m *MockClient) RequestVote(ctx context.Context, req *pb.RequestVoteReq, opts ...grpc.CallOption) (*pb.RequestVoteRes, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.vote_functor(ctx, req)
}

func (m *MockClient) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq, opts ...grpc.CallOption) (*pb.AppendEntriesRes, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.ae_functor(ctx, req)
}

func (m *MockClient) ReplaceVoteFunctor(f VoteFunctor) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.vote_functor = f
}

func (m *MockClient) ReplaceAeFunctor(f AeFunctor) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.ae_functor = f
}

func startup_peer_test(t *testing.T) *TestContext {
	result := &TestContext{}
	tmp := &badger.BadgerEngine{}
	result.dir = fmt.Sprintf("/tmp/%s", utils.RandStringRunes(16))
	if err := tmp.Init(result.dir); err != nil {
		t.Fatalf("init dir:%s failed:%v", result.dir, err)
	}
	result.svr = &PeerServer{}
	result.engine = tmp
	err := result.svr.Init("id0",
		"127.0.0.1:10000",
		map[string]string{
			"id0": "127.0.0.1:10000",
			"id1": "127.0.0.1:10001",
			"id2": "127.0.0.1:10002",
		},
		tmp,
		300*time.Millisecond)
	if err != nil {
		t.Fatalf("init server failed:%v", err)
	}
	return result
}

func teardown_peer_test(t *testing.T, test_ctx *TestContext) {
	test_ctx.svr.Stop()
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func test_peer_term(t *testing.T, test_ctx *TestContext) {
	if tmp := test_ctx.svr.GetTerm(); tmp != 0 {
		t.Errorf("initial term not 0:%d", tmp)
		return
	}
	if false != test_ctx.svr.UpdateTerm(0) {
		t.Errorf("update term with same term should fail")
		return
	}
	if true != test_ctx.svr.UpdateTerm(1) {
		t.Errorf("update term with greater term should success")
		return
	}
	if tmp := test_ctx.svr.GetTerm(); tmp != 1 {
		t.Errorf("get term:%d", tmp)
	}
	if tmp, err := test_ctx.svr.store.GetTerm(); tmp != 1 || err != nil {
		t.Errorf("persist term: should be 1:%v %v", tmp, err)
	}
	if err := test_ctx.svr.voteInLock("abc"); err != nil {
		t.Errorf("votefor abc failed:%v", err)
	}
	if _, tmp := test_ctx.svr.GetTermAndVoteFor(); tmp != "abc" {
		t.Errorf("current votefor should be abc:%s", tmp)
	}
	if tmp, err := test_ctx.svr.store.GetVoteFor(); err != nil || tmp != "abc" {
		t.Errorf("persisted votefor should be abc:%v %v", tmp, err)
	}
	if true != test_ctx.svr.UpdateTerm(2) {
		t.Errorf("update term with greater term should success")
		return
	}
	if tmp := test_ctx.svr.GetTerm(); tmp != 2 {
		t.Errorf("get term:%d", tmp)
	}
	if _, tmp := test_ctx.svr.GetTermAndVoteFor(); tmp != "" {
		t.Errorf("current votefor should be abc:%s", tmp)
	}
	if tmp, err := test_ctx.svr.store.GetVoteFor(); err != nil || tmp != "" {
		t.Errorf("persisted votefor should be \"\":%v %v", tmp, err)
	}
}

func test_grant_vote(t *testing.T, test_ctx *TestContext) {
	if ok := test_ctx.svr.UpdateTerm(1); !ok {
		t.Errorf("UpdateTerm failed")
		return
	}
	if err := test_ctx.svr.store.AppendLogEntry(&pb.LogEntry{
		Index: 1,
		Term:  1,
		Key:   "a",
		Value: []byte("a"),
	}); err != nil {
		t.Errorf("append entry 1 failed:%v", err)
	}
	if err := test_ctx.svr.store.AppendLogEntry(&pb.LogEntry{
		Index: 2,
		Term:  1,
		Key:   "a",
		Value: []byte("a"),
	}); err != nil {
		t.Errorf("append entry 1 failed:%v", err)
	}

	req := &pb.RequestVoteReq{}
	req.Term = 0
	req.CandidateId = "abc"
	if _, err_msg := test_ctx.svr.GrantVote(req); err_msg == "" {
		t.Errorf("GrantVote with smaller term should fail")
	}
	req.Term = 1
	req.LastLogTerm = 1
	req.LastLogIndex = 1
	req.CandidateId = "abc"
	if _, err_msg := test_ctx.svr.GrantVote(req); err_msg != "log not as uptodate" {
		t.Errorf("GrantVote with not uptodate log invalid err:%s", err_msg)
	}

	req.Term = 2
	req.LastLogTerm = 1
	req.LastLogIndex = 2
	req.CandidateId = "abc"
	if ok, err_msg := test_ctx.svr.GrantVote(req); err_msg != "" || !ok {
		t.Errorf("GrantVote with uptodate log should not fail[%v:%s]", ok, err_msg)
		return
	}
	if test_ctx.svr.vote_for != "abc" {
		t.Errorf("after grant vote, my vote_for not change:%s", test_ctx.svr.vote_for)
	}
	if test_ctx.svr.current_term != 2 {
		t.Errorf("after grant vote, my current_term not change:%d", test_ctx.svr.current_term)
	}
}

func test_commit_index(t *testing.T, test_ctx *TestContext) {
	if idx := test_ctx.svr.GetCommitIndex(); idx != -1 {
		t.Errorf("initial commit idx not -1:%d", idx)
	}
	test_ctx.svr.UpdateCommitIndex(2)
	if idx := test_ctx.svr.GetCommitIndex(); idx != 2 {
		t.Errorf("after update commit idx not 2:%d", idx)
	}
	if idx, err := test_ctx.svr.store.GetCommitIndex(); err != nil || idx != 2 {
		t.Errorf("after update store commit idx not 2:[%v,%v]", idx, err)
	}
}

func TestPeerTerm(t *testing.T) {
	ctx := startup_peer_test(t)
	defer teardown_peer_test(t, ctx)
	test_peer_term(t, ctx)
}

func TestGrantVote(t *testing.T) {
	ctx := startup_peer_test(t)
	defer teardown_peer_test(t, ctx)
	test_grant_vote(t, ctx)
}

func TestCommitIndex(t *testing.T) {
	ctx := startup_peer_test(t)
	defer teardown_peer_test(t, ctx)
	test_commit_index(t, ctx)
}
