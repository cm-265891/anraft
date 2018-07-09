package peer

import (
    "anraft/storage"
)

type SectionPrefix int8

const (
    META SectionPrefix = itoa
    LOG
    KV
)

type PeerStorage struct {
    engine *storage.Storage
}

// NOTE(deyukong): It's confusion to handle endians, so I convert nums to strings with preceding zeros
func (p *PeerStorage) SaveTerm(term int64) error {
    key := fmt.Sprintf("%03d|TERM", META)
    val := fmt.Sprintf("%d", term)
    return p.engine.Set([]byte(key), []byte(val))
}

func (p *PeerStorage) SaveVoteFor(vote_for string) error {
    key := fmt.Sprintf("%03d|VOTEFOR", META)
    return p.engine.Set([]byte(key), []byte(vote_for))
}
