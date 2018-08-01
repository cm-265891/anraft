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

type MockClient struct {
	pb.PeerClient
	mutex        sync.Mutex
	vote_functor func(context.Context, *pb.RequestVoteReq) (*pb.RequestVoteRes, error)
}

func (m *MockClient) RequestVote(ctx context.Context, req *pb.RequestVoteReq, opts ...grpc.CallOption) (*pb.RequestVoteRes, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.vote_functor(ctx, req)
}

func (m *MockClient) ReplaceFunctor(f func(context.Context, *pb.RequestVoteReq) (*pb.RequestVoteRes, error)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.vote_functor = f
}

func startup_candidate_test(t *testing.T) *TestContext {
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
	for _, o := range result.svr.cluster_info {
		o.client = &MockClient{}
	}
	return result
}

func teardown_candidate_test(t *testing.T, test_ctx *TestContext) {
	test_ctx.svr.Stop()
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func TestCandidateVoteFail(t *testing.T) {
	ctx := startup_candidate_test(t)
	defer teardown_candidate_test(t, ctx)
	for _, o := range ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		oo.ReplaceFunctor(func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			rsp := new(pb.RequestVoteRes)
			rsp.Header = new(pb.ResHeader)
			rsp.Term = int64(1)
			rsp.VoteGranted = "id1"
			return rsp, nil
		})
	}
	go ctx.svr.Elect()
	time.Sleep(1 * time.Second)
	if ctx.svr.state != pb.PeerState_Follower {
		t.Errorf("peer should become follower after elect fail:%v", ctx.svr.state)
	}
}

func TestCandidateVoteSuccess(t *testing.T) {
	ctx := startup_candidate_test(t)
	defer teardown_candidate_test(t, ctx)
	for _, o := range ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		oo.ReplaceFunctor(func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			rsp := new(pb.RequestVoteRes)
			rsp.Header = new(pb.ResHeader)
			rsp.Term = int64(1)
			rsp.VoteGranted = "id0"
			return rsp, nil
		})
	}
	go ctx.svr.Elect()
	time.Sleep(1 * time.Second)
	if ctx.svr.state != pb.PeerState_Leader {
		t.Errorf("peer should become leader after elect succ:%v", ctx.svr.state)
	}
}

func TestCandidateVoteDraw(t *testing.T) {
	ctx := startup_candidate_test(t)
	defer teardown_candidate_test(t, ctx)
	for _, o := range ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		// holy shit
		oo.ReplaceFunctor(func(id string) func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			return func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
				rsp := new(pb.RequestVoteRes)
				rsp.Header = new(pb.ResHeader)
				rsp.Term = req.Term
				rsp.VoteGranted = id
				return rsp, nil
			}
		}(o.id))
	}
	// make it donot retry
	go ctx.svr.Elect()
	time.Sleep(3 * time.Second)
	if ctx.svr.state != pb.PeerState_Candidate {
		t.Errorf("peer should keep candidate after elect draw:%v", ctx.svr.state)
	}
}
