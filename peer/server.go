package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/proto/server_proto"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
)

/* handles client requests */

func (p *PeerServer) ForwardSet(ctx context.Context, req *server_proto.SetReq) (*server_proto.SetRes, error) {
	return nil, nil
}

func (p *PeerServer) Set(ctx context.Context, req *server_proto.SetReq) (*server_proto.SetRes, error) {
	write_functor := func() (int64, int64, bool) {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		if p.state != pb.PeerState_Leader {
			return -1, -1, false
		}
		last_entry, err := p.store.GetLastLogEntry()
		if err != nil {
			log.Fatalf("get last entry failed:%v", err)
		}

		index := int64(0)
		if last_entry != nil {
			index = last_entry.Index + 1
		}

		entry := &pb.LogEntry{
			Index: index,
			Term:  p.current_term,
			Op:    pb.LogOp_Set,
			Key:   req.Key,
			Value: req.Value,
		}
		if err := p.store.AppendLogEntry(entry); err != nil {
			log.Fatalf("leader append entry:%v failed:%v", entry, err)
		}
		return index, p.current_term, true
	}

	retry_cnt := 0
	for {
		if retry_cnt > 3 {
			log.Fatalf("WaitCommit retry 3 times failed!!!")
		}
		idx, term, wrote := write_functor()
		if !wrote {
			return p.ForwardSet(ctx, req)
		}
		if err := p.WaitCommitApply(idx, term); err == nil {
			return &server_proto.SetRes{
				Header: &pb.ResHeader{},
			}, nil
		} else {
			retry_cnt += 1
			log.Warnf("leader write:%v wait failed:%v", req, err)
		}
	}
	log.Fatalf("BUG:never reach here")
	return nil, nil
}
