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

func startup_leader_test(t *testing.T) *TestContext {
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

func teardown_leader_test(t *testing.T, test_ctx *TestContext) {
	test_ctx.svr.Stop()
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func TestForwardCommitIdx(t *testing.T) {
	test_ctx := startup_leader_test(t)
	defer teardown_leader_test(t, test_ctx)

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

	commit_fwder := &CommitForwarder{}
	commit_fwder.Init(test_ctx.svr.cluster_info, "id0")
	test_ctx.svr.ForwardCommitIndex(commit_fwder,
		&AeWrapper{
			id:  "id1",
			res: nil,
			it: &IndexAndTerm{
				idx:  int64(1),
				term: int64(1),
			},
		})
	if test_ctx.svr.commit_index != 1 {
		t.Errorf("commit_index not ok:%v", test_ctx.svr.commit_index)
		return
	}
	test_ctx.svr.ForwardCommitIndex(commit_fwder,
		&AeWrapper{
			id:  "id2",
			res: nil,
			it: &IndexAndTerm{
				idx:  int64(1),
				term: int64(1),
			},
		})
	if test_ctx.svr.commit_index != 1 {
		t.Errorf("commit_index not ok:%v", test_ctx.svr.commit_index)
		return
	}
	test_ctx.svr.ForwardCommitIndex(commit_fwder,
		&AeWrapper{
			id:  "id1",
			res: nil,
			it: &IndexAndTerm{
				idx:  int64(2),
				term: int64(1),
			},
		})
	if test_ctx.svr.commit_index != 2 {
		t.Errorf("commit_index not ok:%v", test_ctx.svr.commit_index)
		return
	}
}

func TestHeartBeat(t *testing.T) {
	test_ctx := startup_leader_test(t)
	defer teardown_leader_test(t, test_ctx)
	for _, o := range test_ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		oo.ReplaceAeFunctor(func(context.Context, *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
			rsp := new(pb.AppendEntriesRes)
			rsp.Header = new(pb.ResHeader)
			rsp.Term = int64(2)
			rsp.Result = int32(AE_SMALL_TERM)
			return rsp, nil
		})
	}
	test_ctx.svr.election_timeout = 10 * time.Second
	go test_ctx.svr.LeaderCron(LM_HB)
	time.Sleep(1 * time.Second)
	if test_ctx.svr.state != pb.PeerState_Follower || test_ctx.svr.current_term != int64(2) {
		t.Errorf("heartbeat greater term should be follower:[%v %v]",
			test_ctx.svr.state, test_ctx.svr.current_term)
		return
	}
}

func TestTransLogOK(t *testing.T) {
	test_ctx := startup_leader_test(t)
	defer teardown_leader_test(t, test_ctx)
	for _, o := range test_ctx.svr.cluster_info {
		oo, _ := o.client.(*MockClient)
		oo.ReplaceAeFunctor(func(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
			rsp := new(pb.AppendEntriesRes)
			rsp.Header = new(pb.ResHeader)
			rsp.Term = req.Term
			rsp.Result = int32(AE_OK)
			return rsp, nil
		})
	}

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

	test_ctx.svr.election_timeout = 10 * time.Second
	test_ctx.svr.state = pb.PeerState_Leader
	go test_ctx.svr.LeaderCron(LM_TL)
	time.Sleep(1 * time.Second)
	if test_ctx.svr.state != pb.PeerState_Leader || test_ctx.svr.commit_index != int64(2) {
		t.Errorf("invalid translog state:%v %d", test_ctx.svr.state, test_ctx.svr.commit_index)
		return
	}
}
