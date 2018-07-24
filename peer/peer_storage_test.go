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
}

var test_ctx *TestContext = nil

func startup(t *testing.T) {
	result := &TestContext{}
	tmp := &badger.BadgerEngine{}
	result.dir = fmt.Sprintf("/tmp/%s", utils.RandStringRunes(16))
	if err := tmp.Init(result.dir); err != nil {
		t.Fatalf("init dir:%s failed:%v", result.dir, err)
	}
	result.engine = tmp
	result.api = &PeerStorage{}
	result.api.Init(result.engine)
	test_ctx = result
}

func teardown(t *testing.T) {
	test_ctx.engine.Close()
	os.RemoveAll(test_ctx.dir)
}

func test_term(t *testing.T) {
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

func test_vote_for(t *testing.T) {
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

func test_commit_idx(t *testing.T) {
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

func test_log(t *testing.T) {
	_, err := test_ctx.api.GetLogEntry(1)
	if err != storage.ErrKeyNotFound {
		t.Errorf("GetLogEntry failed:%v", err)
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
	err = test_ctx.api.AppendLogEntry(&pb.LogEntry{
		Index: 2,
		Term:  1,
		Key:   "a",
		Value: []byte("a"),
	})
	if err != nil {
		t.Errorf("append entry 2 failed:%v", err)
	}
	it1 := test_ctx.api.SeekLogAt(0)
	defer it1.Close()
	it1_cnt := 0
	for it1.ValidForPrefix(LOG_PREFIX) {
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
		it4_cnt += 1
		it4.Next()
	}
	if it4_cnt != 0 {
		t.Errorf("it4_cnt should be 0")
	}
}

func TestStorage(t *testing.T) {
	startup(t)
	defer teardown(t)
	t.Run("test_term", test_term)
	t.Run("test_vote_for", test_vote_for)
	t.Run("test_commit_idx", test_commit_idx)
	t.Run("test_log", test_log)
}
