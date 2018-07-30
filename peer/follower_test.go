package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage/badger"
	"anraft/utils"
	"fmt"
	"os"
	"testing"
	"time"
)

func startup_follower_test(t *testing.T) *TestContext {
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

func teardown_follower_test(t *testing.T, test_ctx *TestContext) {
	test_ctx.svr.Stop()
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func TestFollowerTimeout(t *testing.T) {
	ctx := startup_follower_test(t)
	defer teardown_follower_test(t, ctx)
	go ctx.svr.FollowerCron()
	time.Sleep(1 * time.Second)
	if ctx.svr.state != pb.PeerState_Candidate {
		t.Errorf("peer should become candidate after elect timeout:%v", ctx.svr.state)
	}
}

func TestFollowerAppendEntry(t *testing.T) {
	ctx := startup_follower_test(t)
	defer teardown_follower_test(t, ctx)
	go ctx.svr.FollowerCron()

	// 1. prevLogIndex not found with -1
	entry := &pb.AppendEntriesReq{
		Header:       &pb.ReqHeader{},
		Term:         int64(1),
		LeaderId:     "id1",
		PrevLogIndex: int64(1),
		PrevLogTerm:  int64(1),
		Entries: []*pb.LogEntry{
			&pb.LogEntry{
				Index: int64(1),
				Term:  int64(1),
				Key:   "a",
				Value: []byte("b"),
			},
		},
		LeaderCommit: int64(-1),
	}
	ctx.svr.new_entry_pair.input <- entry
	output := <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_TERM_UNMATCH) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 2. append success
	// (1, 1)
	entry.PrevLogIndex = int64(-1)
	entry.PrevLogTerm = int64(-1)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_OK) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 3. append again with same entry
	// (1, 1)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_OK) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 4. append another valid entry
	// (1, 1), (2, 1)
	entry.PrevLogIndex = int64(1)
	entry.PrevLogTerm = int64(1)
	entry.Entries[0].Index = int64(2)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_OK) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 4.1 append again with same entry, but not -1
	// (1, 1,), (2, 1)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_OK) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 5. prevLogIndex not found
	// (1, 1), (2, 1)
	entry.PrevLogIndex = int64(3)
	entry.PrevLogTerm = int64(1)
	entry.Entries[0].Index = int64(4)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_TERM_UNMATCH) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 6. prevLogTerm not match
	// (1, 1), (2, 1)
	entry.PrevLogIndex = int64(1)
	entry.PrevLogTerm = int64(2)
	entry.Entries[0].Index = int64(4)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_TERM_UNMATCH) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 7. append with overwrite
	// (1, 1), (2, 2)
	entry.PrevLogIndex = int64(1)
	entry.PrevLogTerm = int64(1)
	entry.Term = int64(2)
	entry.Entries[0].Index = int64(2)
	entry.Entries[0].Term = int64(2)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_OK) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}

	// 8. append with smaller term
	entry.PrevLogIndex = int64(-1)
	entry.PrevLogTerm = int64(-1)
	entry.Term = int64(1)
	entry.Entries[0].Index = int64(1)
	entry.Entries[0].Term = int64(1)
	ctx.svr.new_entry_pair.input <- entry
	output = <-ctx.svr.new_entry_pair.output
	if output.Result != int32(AE_SMALL_TERM) {
		t.Errorf("appendentry bad result:%v", output)
		return
	}
}
