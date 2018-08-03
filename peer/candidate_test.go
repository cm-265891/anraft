package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage/badger"
	"anraft/utils"
	"fmt"
	context "golang.org/x/net/context"
	//"google.golang.org/grpc"
	"os"
	//"sync"
	"testing"
	"time"
)

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
		oo.ReplaceVoteFunctor(func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
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
		oo.ReplaceVoteFunctor(func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
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
		oo.ReplaceVoteFunctor(func(id string) func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			return func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
				rsp := new(pb.RequestVoteRes)
				rsp.Header = new(pb.ResHeader)
				rsp.Term = req.Term
				rsp.VoteGranted = id
				return rsp, nil
			}
		}(o.id))
	}
	go ctx.svr.Elect()
	time.Sleep(3 * time.Second)
	if ctx.svr.state != pb.PeerState_Candidate {
		t.Errorf("peer should keep candidate after elect draw:%v", ctx.svr.state)
	}
}

func TestCandidateRecvOtherVote(t *testing.T) {
	ctx := startup_candidate_test(t)
	defer teardown_candidate_test(t, ctx)
	ctx.svr.election_timeout = 3 * time.Second
	for _, o := range ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		// holy shit
		oo.ReplaceVoteFunctor(func(id string) func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			return func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
				// mock timeout
				time.Sleep(3 * time.Second)
				rsp := new(pb.RequestVoteRes)
				rsp.Header = new(pb.ResHeader)
				rsp.Term = req.Term
				rsp.VoteGranted = id
				return rsp, nil
			}
		}(o.id))
	}
	go ctx.svr.Elect()
	req := new(pb.RequestVoteReq)
	req.Header = new(pb.ReqHeader)
	req.Term = 2
	req.CandidateId = "id1"
	req.LastLogIndex = -1
	req.LastLogTerm = -1
	ctx.svr.vote_pair.input <- req
	output := <-ctx.svr.vote_pair.output
	if output.VoteGranted != "id1" || output.Term != 2 {
		t.Errorf("bad vote result:%v", output)
		return
	}
	time.Sleep(1 * time.Second)
	if ctx.svr.state != pb.PeerState_Follower {
		t.Errorf("after success vote, peer should be follower:%v", ctx.svr.state)
	}
}

func TestCandidateRecvNewEntry(t *testing.T) {
	ctx := startup_candidate_test(t)
	defer teardown_candidate_test(t, ctx)
	ctx.svr.election_timeout = 3 * time.Second
	for _, o := range ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		// holy shit
		oo.ReplaceVoteFunctor(func(id string) func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
			return func(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
				// mock timeout
				time.Sleep(3 * time.Second)
				rsp := new(pb.RequestVoteRes)
				rsp.Header = new(pb.ResHeader)
				rsp.Term = req.Term
				rsp.VoteGranted = id
				return rsp, nil
			}
		}(o.id))
	}
	go ctx.svr.Elect()
	req := new(pb.AppendEntriesReq)
	req.Header = new(pb.ReqHeader)
	req.Term = int64(0)
	req.LeaderId = "id1"
	req.LeaderCommit = -1
	req.PrevLogIndex = -1
	req.PrevLogTerm = -1
	ctx.svr.new_entry_pair.input <- req
	output := <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_SMALL_TERM) {
		t.Errorf("bad entry result:%v", output)
	}
	req.Term = int64(1)
	ctx.svr.new_entry_pair.input <- req
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_RETRY) {
		t.Errorf("bad entry result:%v", output)
	}
}
