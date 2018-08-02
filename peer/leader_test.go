package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage/badger"
	"anraft/utils"
	"fmt"
	//context "golang.org/x/net/context"
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
