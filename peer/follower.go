package peer

import (
	pb "anraft/proto/peer_proto"
	"anraft/storage"
	"anraft/utils"
	"github.com/ngaut/log"
	"time"
)

func (p *PeerServer) ElecTimeout() {
	if p.state != pb.PeerState_Follower {
		log.Fatalf("ElecTimeout in wrong state:%v", p.state)
	}
	p.Elect()
}

func (p *PeerServer) FollowerCron() {
	tchan := time.After(p.election_timeout)
	for {
		select {
		case <-p.closer.HasBeenClosed():
			log.Infof("follower cron stops...")
			return
		case <-tchan:
			p.ElecTimeout()
			return
		case tmp := <-p.new_entry_pair.input:
			p.new_entry_pair.output <- p.appendNewEntries(tmp)
			tchan = time.After(p.election_timeout)
		case tmp := <-p.vote_pair.input:
			if ok, reason := p.GrantVote(tmp); !ok {
				log.Infof("follower got vote, now_term:%d not grant:%s", p.current_term, reason)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					// although vote not granted, it is still guaranteed that if other's term is greater,
					// ours is also updated to other's
					Term: p.current_term,
				}
			} else {
				log.Infof("follower got vote and granted, current_term:%d", p.current_term)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					Term:        p.current_term,
				}
			}
		}
	}
}

// TODO(deyukong): check about my commit-point and first-log
func (p *PeerServer) appendNewEntries(entries *pb.AppendEntriesReq) *pb.AppendEntriesRes {
	// it's the upper logic's duty to guarantee the correct role at this point
	if p.state != pb.PeerState_Follower {
		log.Fatalf("append entry but role[%v] not follower", p.state)
	}
	if p.current_term > entries.Term {
		log.Infof("append entry but smaller term[%d:%d]", p.current_term, entries.Term)
		return &pb.AppendEntriesRes{
			Header: new(pb.ResHeader),
			Term:   p.current_term,
			Result: int32(AE_SMALL_TERM),
		}
	}

	p.UpdateTerm(entries.Term)
	if len(entries.Entries) == 0 {
		// heartbeat
		return &pb.AppendEntriesRes{
			Header: new(pb.ResHeader),
			Term:   p.current_term,
			Result: int32(AE_OK),
		}
	}

	var iter storage.Iterator = nil
	if entries.PrevLogIndex == -1 {
		// NOTE(deyukong): currently, committed logs are not archieved, so seek from 0
		iter = p.store.SeekLogAt(0)
		defer iter.Close()
	} else {
		iter = p.store.SeekLogAt(entries.PrevLogIndex)
		defer iter.Close()

		// seek to the end
		if !iter.ValidForPrefix(LOG_PREFIX) {
			return &pb.AppendEntriesRes{
				Header: new(pb.ResHeader),
				Term:   entries.Term,
				Result: int32(AE_TERM_UNMATCH),
			}
		}
		entry, err := IterEntry2Log(iter)
		if err != nil {
			log.Fatalf("idx:%d parse rawlog to logentry failed:%v", entries.PrevLogIndex, err)
		}
		// entry not match
		if entry.Term != entries.PrevLogTerm {
			log.Warnf("master[%s] entry:[%d:%d] not match mine[%d:%d]",
				entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm, entry.Index, entry.Term)
			return &pb.AppendEntriesRes{
				Header: new(pb.ResHeader),
				Term:   entries.Term,
				Result: int32(AE_TERM_UNMATCH),
			}
		}
		// prev_entry matches, move iter to the next
		iter.Next()
	}

	// at most times, code reaches here with iter invalid
	if iter.ValidForPrefix(LOG_PREFIX) {
		log.Warnf("master[%s] replays old log:[%d:%d]",
			entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm)
	}

	// set apply_from default to len(entries.Entries), if all matches, we have nothing to apply
	apply_from := len(entries.Entries)
	has_conflict := false
	for idx, master_entry := range entries.Entries {
		// seek to the end
		if !iter.ValidForPrefix(LOG_PREFIX) {
			apply_from = idx
			break
		}
		entry, err := IterEntry2Log(iter)
		if err != nil {
			// NOTE(deyukong): master_entry.Index is not mine, but it should be the same,
			// just for problem-tracking
			log.Fatalf("idx:%d parse rawlog to logentry failed:%v", master_entry.Index, err)
		}
		if entry.Index != master_entry.Index {
			log.Fatalf("bug:master[%s] entry:[%d:%d] index defers from  mine[%d:%d]",
				entries.LeaderId, entries.PrevLogIndex, entries.PrevLogTerm, entry.Index, entry.Term)
		}
		if entry.Term != master_entry.Term {
			apply_from = idx
			has_conflict = true
			break
		}
		// entry.Term == master_entry.Term
		iter.Next()
	}

	// at most times, code reaches here with iter invalid
	for ; has_conflict && iter.ValidForPrefix(LOG_PREFIX); iter.Next() {
		entry, err := IterEntry2Log(iter)
		if err != nil {
			log.Fatalf("logentry parse failed:%v", err)
		}
		if err := p.store.DelLogEntry(entry); err != nil {
			log.Fatalf("del logentry:%v failed:%v", entry, err)
		} else {
			log.Infof("logentry:%v remove succ", entry)
		}
	}

	for i := apply_from; i < len(entries.Entries); i++ {
		if err := p.store.AppendLogEntry(entries.Entries[i]); err != nil {
			log.Fatalf("apply entry:%v to store failed:%v", entries.Entries[i], err)
		} else {
			log.Infof("apply entry:%v to store success", entries.Entries[i])
		}
	}

	// update my commit index
	if p.commit_index > entries.LeaderCommit {
		log.Fatalf("my commitidx:%d shouldnt be greater than master's:%d", p.commit_index, entries.LeaderCommit)
	}

	new_commit := utils.Int64Min(entries.LeaderCommit, entries.Entries[len(entries.Entries)-1].Index)
	// it may happens when master re-apply old logs which have already been applied.
	if new_commit < p.commit_index {
		last_entry, err := p.store.GetLastLogEntry()
		if err != nil || last_entry == nil {
			log.Fatalf("get last entry failed:%v", err)
		}
		log.Warnf("new_commit:%d less than mine:%d, last_idx:%d", new_commit, p.commit_index, last_entry.Index)
	} else {
		// commit_idx should be monotonic
		p.UpdateCommitIndex(new_commit)
	}
	return &pb.AppendEntriesRes{
		Header: new(pb.ResHeader),
		Term:   entries.Term,
		Result: int32(AE_OK),
	}
}
