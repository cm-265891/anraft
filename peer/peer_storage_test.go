package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"anraft/storage/badger"
	"anraft/utils"
	"fmt"
	"os"
	"testing"
)

type TestContext struct {
	engine storage.Storage
	dir    string
	api    *PeerStorage
	svr    *PeerServer
}

func startup_peer_storage_test(t *testing.T) *TestContext {
	result := &TestContext{}
	tmp := &badger.BadgerEngine{}
	result.dir = fmt.Sprintf("/tmp/%s", utils.RandStringRunes(16))
	if err := tmp.Init(result.dir); err != nil {
		t.Fatalf("init dir:%s failed:%v", result.dir, err)
	}
	result.engine = tmp
	result.api = &PeerStorage{}
	result.api.Init(result.engine)
	return result
}

func teardown_peer_storage_test(t *testing.T, test_ctx *TestContext) {
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func test_term(t *testing.T, test_ctx *TestContext) {
	term, err := test_ctx.api.GetTerm()
	if err != nil {
		t.Errorf("getTerm failed:%v", err)
		return
	}
	if term != 0 {
		t.Errorf("term init not 0:%d", term)
		return
	}
	err = test_ctx.api.SaveTerm(100)
	term, err = test_ctx.api.GetTerm()
	if err != nil {
		t.Errorf("getTerm failed:%v", err)
		return
	}
	if term != 100 {
		t.Errorf("term not 100:%d", term)
		return
	}
}

func test_vote_for(t *testing.T, test_ctx *TestContext) {
	vote_for, err := test_ctx.api.GetVoteFor()
	if err != nil {
		t.Errorf("GetVoteFor failed:%v", err)
		return
	}
	if vote_for != "" {
		t.Errorf("vote_for init not empty:%s", vote_for)
		return
	}
	err = test_ctx.api.SaveVoteFor("abc")
	vote_for, err = test_ctx.api.GetVoteFor()
	if err != nil {
		t.Errorf("GetVoteFor failed:%v", err)
		return
	}
	if vote_for != "abc" {
		t.Errorf("term not abc:%s", vote_for)
		return
	}
}

func test_commit_idx(t *testing.T, test_ctx *TestContext) {
	commit_idx, err := test_ctx.api.GetCommitIndex()
	if err != nil {
		t.Errorf("GetCommitIndex failed:%v", err)
		return
	}
	if commit_idx != -1 {
		t.Errorf("commit_idx init not -1:%d", commit_idx)
		return
	}
	err = test_ctx.api.SaveCommitIndex(1)
	commit_idx, err = test_ctx.api.GetCommitIndex()
	if err != nil {
		t.Errorf("GetCommitIndex failed:%v", err)
		return
	}
	if commit_idx != 1 {
		t.Errorf("commit_idx not 1:%d", commit_idx)
		return
	}
}

func test_log(t *testing.T, test_ctx *TestContext) {
	_, err := test_ctx.api.GetLogEntry(1)
	if err != storage.ErrKeyNotFound {
		t.Errorf("GetLogEntry failed:%v", err)
		return
	}

	entry, err := test_ctx.api.GetLastLogEntry()
	if entry != nil || err != nil {
		t.Errorf("GetLastLogEntry on empty set not pass")
		return
	}

	it := test_ctx.api.SeekLogAt(1)
	defer it.Close()
	if it.ValidForPrefix(LOG_PREFIX) {
		t.Errorf("iterator should not work to empty data")
		return
	}

	err = test_ctx.api.AppendLogEntry(&pb.LogEntry{
		Index: 1,
		Term:  1,
		Key:   "a",
		Value: []byte("a"),
	})
	if err != nil {
		t.Errorf("append entry 1 failed:%v", err)
	}

	_, err = test_ctx.api.GetLogEntry(1)
	if err != nil {
		t.Errorf("GetLogEntry failed:%v", err)
		return
	}

	entry, err = test_ctx.api.GetLastLogEntry()
	if err != nil || entry.Index != 1 {
		t.Errorf("GetLastLogEntry index not 1")
		return
	}

	err = test_ctx.api.AppendLogEntry(&pb.LogEntry{
		Index: 2,
		Term:  1,
		Key:   "a",
		Value: []byte("a"),
	})
	if err != nil {
		t.Errorf("append entry 2 failed:%v", err)
	}

	entry, err = test_ctx.api.GetLastLogEntry()
	if err != nil || entry.Index != 2 {
		t.Errorf("GetLastLogEntry index not 2")
		return
	}

	it1 := test_ctx.api.SeekLogAt(0)
	defer it1.Close()
	it1_cnt := 0
	for it1.ValidForPrefix(LOG_PREFIX) {
		_, err := IterEntry2Log(it1)
		if err != nil {
			t.Errorf("IterEntry2Log should not fail")
		}
		it1_cnt += 1
		it1.Next()
	}
	if it1_cnt != 2 {
		t.Errorf("it1_cnt should be 2")
	}
	it2 := test_ctx.api.SeekLogAt(1)
	defer it2.Close()
	it2_cnt := 0
	for it2.ValidForPrefix(LOG_PREFIX) {
		_, err := IterEntry2Log(it2)
		if err != nil {
			t.Errorf("IterEntry2Log should not fail")
		}
		it2_cnt += 1
		it2.Next()
	}
	if it2_cnt != 2 {
		t.Errorf("it2_cnt should be 2")
	}
	it3 := test_ctx.api.SeekLogAt(2)
	defer it3.Close()
	it3_cnt := 0
	for it3.ValidForPrefix(LOG_PREFIX) {
		_, err := IterEntry2Log(it3)
		if err != nil {
			t.Errorf("IterEntry2Log should not fail")
		}
		it3_cnt += 1
		it3.Next()
	}
	if it3_cnt != 1 {
		t.Errorf("it3_cnt should be 1")
	}
	it4 := test_ctx.api.SeekLogAt(3)
	defer it4.Close()
	it4_cnt := 0
	for it4.ValidForPrefix(LOG_PREFIX) {
		_, err := IterEntry2Log(it4)
		if err != nil {
			t.Errorf("IterEntry2Log should not fail")
		}
		it4_cnt += 1
		it4.Next()
	}
	if it4_cnt != 0 {
		t.Errorf("it4_cnt should be 0")
	}

	err = test_ctx.api.DelLogEntry(&pb.LogEntry{
		Index: 2,
	})
	if err != nil {
		t.Errorf("del logentry:2 failed:%v", err)
		return
	}
	entry, err = test_ctx.api.GetLastLogEntry()
	if err != nil || entry.Index != 1 {
		t.Errorf("GetLastLogEntry index not 1")
		return
	}
}

func TestStorage(t *testing.T) {
	ctx := startup_peer_storage_test(t)
	defer teardown_peer_storage_test(t, ctx)
	test_term(t, ctx)
	test_vote_for(t, ctx)
	test_commit_idx(t, ctx)
	test_log(t, ctx)
}
