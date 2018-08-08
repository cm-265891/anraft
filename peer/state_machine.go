package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/utils"
	"github.com/ngaut/log"
	"sync"
	"time"
)

type StateMachine struct {
	store             *PeerStorage
	svr               *PeerServer
	mutex             sync.RWMutex
	closer            *utils.Closer
	commit_apply      int64
	commit_apply_cond *sync.Cond
}

func (s *StateMachine) Init(svr *PeerServer, store *PeerStorage, closer *utils.Closer) {
	s.svr = svr
	s.store = store
	s.closer = closer
	s.commit_apply_cond = sync.NewCond(&s.mutex)
	commit_apply, err := s.store.GetCommitApply()
	if err != nil {
		log.Fatalf("get commit_apply failed:%v", err)
	}
	s.commit_apply = commit_apply
}

func (s *StateMachine) GetCommitApply() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.commit_apply
}

func (s *StateMachine) FetchAndApply(batch int64) int64 {
	commit_idx := s.svr.GetCommitIndex()
	commit_apply := s.GetCommitApply()
	if commit_idx == commit_apply {
		return int64(0)
	}

	cnt := int64(0)
	for i := commit_apply + 1; i <= commit_idx && (i-commit_apply <= batch); i++ {
		entry, err := s.store.GetLogEntry(i)
		if err != nil {
			log.Fatalf("get logentry[%d] failed:%v", i, err)
		}
		if entry.Op == pb.LogOp_Set {
			if err := s.store.SetKV(entry.Key, entry.Value); err != nil {
				log.Fatalf("set kv:%v failed:%v", entry, err)
			}
		} else if entry.Op == pb.LogOp_Del {
			if err := s.store.DelKV(entry.Key); err != nil {
				log.Fatalf("del kv:%v failed:%v", entry, err)
			}
		} else {
			log.Fatalf("invalid logentry:%v", entry)
		}
		cnt += 1
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.commit_apply = commit_apply + cnt
	s.commit_apply_cond.Broadcast()
	return cnt
}

func (s *StateMachine) WaitCommitApply(idx int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for s.commit_apply < idx {
		s.commit_apply_cond.Wait()
	}
}

func (s *StateMachine) Run() {
	s.closer.AddOne()
	defer s.closer.Done()
	for {
		select {
		case <-s.closer.HasBeenClosed():
			log.Infof("statmachine shutting down...")
			return
		default: // nothing
		}
		if n := s.FetchAndApply(100); n == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
