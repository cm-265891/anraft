package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"fmt"
	"github.com/golang/protobuf/proto"
	"math"
)

type SectionPrefix int8

const (
	META SectionPrefix = iota
	LOG
	KV
)

var (
	LOG_PREFIX = []byte(fmt.Sprintf("%03d|", LOG))
	LOG_END    = []byte(fmt.Sprintf("%03d|%021d", LOG, math.MaxInt64))
)

type PeerStorage struct {
	engine storage.Storage
}

// NOTE(deyukong): It's confusion to handle endians, so I convert nums to strings with preceding zeros
func (p *PeerStorage) SaveTerm(term int64) error {
	key := fmt.Sprintf("%03d|CURRENT_TERM", META)
	val := fmt.Sprintf("%d", term)
	return p.engine.Set([]byte(key), []byte(val))
}

func (p *PeerStorage) SaveVoteFor(vote_for string) error {
	key := fmt.Sprintf("%03d|VOTE_FOR", META)
	return p.engine.Set([]byte(key), []byte(vote_for))
}

func (p *PeerStorage) SeekLogAt(index int64) storage.Iterator {
	key := fmt.Sprintf("%03d|%021d", LOG, index)
	return p.engine.Seek([]byte(key), true)
}

func (p *PeerStorage) DelLogEntry(e *pb.LogEntry) error {
	key := fmt.Sprintf("%03d|%021d", LOG, e.Index)
	return p.engine.Del([]byte(key))
}

func (p *PeerStorage) AppendLogEntry(e *pb.LogEntry) error {
	return nil
}

func (p *PeerStorage) GetLastLogEntry() (*pb.LogEntry, error) {
	iter := p.engine.Seek(LOG_END, false)
	// empty log
	if !iter.ValidForPrefix(LOG_PREFIX) {
		return nil, nil
	}
	return IterEntry2Log(iter)
}

func IterEntry2Log(iter storage.Iterator) (*pb.LogEntry, error) {
	if entry, err := iter.Entry(); err != nil {
		return nil, err
	} else {
		result := &pb.LogEntry{}
		if err := proto.Unmarshal(entry.Val, result); err != nil {
			return nil, err
		}
		return result, nil
	}
}
