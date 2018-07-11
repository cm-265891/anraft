package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"math"
	"strconv"
)

type SectionPrefix int8

const (
	META SectionPrefix = iota
	LOG
	KV
)

const (
	CURRENT_TERM = "CURRENT_TERM"
	VOTE_FOR     = "VOTE_FOR"
	COMMIT_IDX   = "COMMIT_IDX"
)

var (
	LOG_PREFIX = []byte(fmt.Sprintf("%03d|", LOG))
	LOG_END    = []byte(fmt.Sprintf("%03d|%021d", LOG, math.MaxInt64))
)

// TODO(deyukong): classify all the store apis, which needs durable, which does not
type PeerStorage struct {
	engine storage.Storage
}

func (p *PeerStorage) Init(engine storage.Storage) {
	p.engine = engine

	// CURRENT_TERM init to 0
	// VOTE_FOR init to ""
	// COMMIT_IDX init to -1
	init_map := map[string][]byte{
		fmt.Sprintf("%03d|%s", META, CURRENT_TERM): []byte(fmt.Sprintf("%d", 0)),
		fmt.Sprintf("%03d|%s", META, VOTE_FOR):     []byte(""),
		fmt.Sprintf("%03d|%s", META, COMMIT_IDX):   []byte(fmt.Sprintf("%d", -1)),
	}
	for k, v := range init_map {
		if _, err := p.engine.Get([]byte(k)); err != nil {
			if err == storage.ErrKeyNotFound {
				if err := p.engine.Set([]byte(k), v); err != nil {
					log.Fatalf("init key:%s failed:%v", k, err)
				}
				log.Infof("init key:%s to %s succ", k, string(v))
			} else {
				log.Fatalf("get key:%s init value failed:%v", k, err)
			}
		}
	}
}

// NOTE(deyukong): It's confusion to handle endians, so I convert nums to strings with preceding zeros
func (p *PeerStorage) SaveTerm(term int64) error {
	key := fmt.Sprintf("%03d|%s", META, CURRENT_TERM)
	val := fmt.Sprintf("%d", term)
	return p.engine.Set([]byte(key), []byte(val))
}

func (p *PeerStorage) GetTerm() (int64, error) {
	key := fmt.Sprintf("%03d|%s", META, CURRENT_TERM)
	if bytes, err := p.engine.Get([]byte(key)); err != nil {
		return 0, err
	} else if term, err := strconv.ParseInt(string(bytes), 10, 64); err != nil {
		return 0, err
	} else {
		return term, nil
	}
}

func (p *PeerStorage) SaveVoteFor(vote_for string) error {
	key := fmt.Sprintf("%03d|%s", META, VOTE_FOR)
	return p.engine.Set([]byte(key), []byte(vote_for))
}

func (p *PeerStorage) GetVoteFor() (string, error) {
	key := fmt.Sprintf("%03d|%s", META, VOTE_FOR)
	if bytes, err := p.engine.Get([]byte(key)); err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}

func (p *PeerStorage) SaveCommitIndex(idx int64) error {
	key := fmt.Sprintf("%03d|%s", META, COMMIT_IDX)
	val := fmt.Sprintf("%d", idx)
	return p.engine.Set([]byte(key), []byte(val))
}

func (p *PeerStorage) GetCommitIndex() (int64, error) {
	key := fmt.Sprintf("%03d|%s", META, COMMIT_IDX)
	if bytes, err := p.engine.Get([]byte(key)); err != nil {
		return 0, err
	} else if term, err := strconv.ParseInt(string(bytes), 10, 64); err != nil {
		return 0, err
	} else {
		return term, nil
	}
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
	key := fmt.Sprintf("%03d|%021d", LOG, e.Index)
	if val, err := proto.Marshal(e); err != nil {
		return err
	} else {
		return p.engine.Set([]byte(key), val)
	}
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
