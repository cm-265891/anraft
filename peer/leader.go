package peer

import (
	pb "anraft/proto/peer_proto"
	"fmt"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
	"sync"
	"time"
)

func heartBeat(p *PeerInfo, term int64, id string, commit_idx int64, timeout time.Duration,
	hb_chan chan *AeWrapper, wg *sync.WaitGroup) {
	defer wg.Done()
	req := new(pb.AppendEntriesReq)
	req.Header = new(pb.ReqHeader)
	req.Term = term
	req.LeaderId = id
	req.LeaderCommit = commit_idx
	req.PrevLogIndex = -1
	req.PrevLogTerm = -1
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	rsp, err := p.client.AppendEntries(ctx, req)
	if err != nil {
		log.Errorf("heartbeat:%v failed:%v", req, err)
		return
	}
	hb_chan <- &AeWrapper{
		id:        id,
		res:       rsp,
		idx: -1, // idx not used for heartbeat
        term: -1, // term not used for heartbeat
	}
}

// term is raft's term, not terminate
func (p *PeerServer) LeaderHeartBeatCron(term_chan chan int64, stop_chan chan int, exit_wg *sync.WaitGroup) {
	defer exit_wg.Done()
	for {
		select {
		case <-stop_chan:
			log.Infof("leader hb stops...")
			return
		case <-time.After(p.election_interval):
			current_term := p.GetTerm()
			hb_chan := make(chan *AeWrapper, len(p.cluster_info)-1)
			var wg sync.WaitGroup
			for _, o := range p.cluster_info {
				if o.host == p.host {
					continue
				}
				wg.Add(1)
				go heartBeat(o, current_term, p.id, p.commit_index, p.election_interval, hb_chan, &wg)
			}
			wg.Wait()
			max_term := current_term
			// NOTE(deyukong): from the aspect of symmetry, master should stepdown if it does not receive
			// hb from the majority. but the paper didnt mention it
			for o := range hb_chan {
				if o.res.Result != int32(AE_OK) {
					log.Infof("get hb from:%s not ok[%d:%s]", o.id, o.res.Result, o.res.Msg)
				}
				if o.res.Term > max_term {
					max_term = o.res.Term
				}
			}
			if max_term > current_term {
				term_chan <- max_term
			}
		}
	}
}

func (p *PeerServer) initLeaderIndexes() (int64, int64) {
    last_entry, err := p.store.GetLastLogEntry()
    if err != nil {
        log.Fatalf("GetLastLogEntry failed:%v", err)
    }
    tmp_n := int64(-1)
    tmp_m := int64(-1)
    if last_entry == nil {
        tmp_n = 0
    } else {
        tmp_n = last_entry.Index + 1
    }
    return tmp_n, tmp_m
}

// TODO(deyukong): currently, we use a sleep(100ms default) to poll master's new entries
// however, it may not provide the minimum latency between leader and follower. Here the
// better choice is to use something like condvars, if client applies new entries, we can
// get immediate signal.
// TODO(deyukong): configure follower apply entry timeout/batchsize
func (p *PeerServer) TransLog(target *PeerInfo, translog_chan chan *AeWrapper, stop_chan chan int, exit_wg *sync.WaitGroup) {
	defer exit_wg.Done()

	next_idx, match_idx := p.initLeaderIndexes()
	// duration default to 100 ms
	duration := time.Duration(100) * time.Millisecond
	batchsize := 100
	// It is guarenteed that master's state and term never changes if this loop is running
	term_snapshot := p.GetTerm()

	for {
		select {
		case <-stop_chan:
			log.Infof("leader to peer:%s translog stops...", target.host)
			return
		case <-time.After(duration):
			last_entry, err := p.store.GetLastLogEntry()
			if err != nil {
				log.Fatalf("GetLastLogEntry failed:%v", err)
			}
			if last_entry == nil || last_entry.Index < next_idx {
				// no new entries, we keep waiting, donot reduce wait-duration
				break
			}
			entries := func() []*pb.LogEntry {
				iter := p.store.SeekLogAt(next_idx)
				defer iter.Close()
				entries := []*pb.LogEntry{}
				for ; iter.ValidForPrefix(LOG_PREFIX) && len(entries) < batchsize; iter.Next() {
					entry, err := IterEntry2Log(iter)
					if err != nil {
						log.Fatalf("IterEntry2Log failed:%v", err)
					}
					entries = append(entries, entry)
				}
				return entries
			}()
			if len(entries) == 0 {
				log.Fatalf("BUG:entries to send shouldnt be empty")
			}
			prev_idx, prev_term := func() (int64, int64) {
				if next_idx == 0 {
					return -1, -1
				}
				entry, err := p.store.GetLogEntry(next_idx - 1)
				if err != nil {
					log.Fatalf("get entry idx:%d failed:%v", next_idx-1, err)
				}
				return entry.Index, entry.Term
			}()

			req := new(pb.AppendEntriesReq)
			req.Header = new(pb.ReqHeader)
			req.Term = term_snapshot
			req.LeaderId = p.id
			if tmp, err := p.store.GetCommitIndex(); err != nil {
				log.Fatalf("GetCommitIndex failed:%v", err)
			} else {
				req.LeaderCommit = tmp
			}
			req.PrevLogIndex = prev_idx
			req.PrevLogTerm = prev_term
			ctx, _ := context.WithTimeout(context.Background(), time.Duration(100)*time.Millisecond)
			rsp, err := target.client.AppendEntries(ctx, req)
			if err != nil {
				log.Warnf("apply entries:%d to %s failed:%v", len(entries), p.host, err)
				break
			}
			if rsp.Result == int32(AE_OK) {
				batch_end_idx := entries[len(entries)-1].Index
				if batch_end_idx < match_idx {
					log.Fatalf("peer:%s batch:%d smaller than match_idx:%d", p.host, batch_end_idx, match_idx)
				}
				match_idx = batch_end_idx
				next_idx = match_idx + 1
				if len(entries) == batchsize {
					duration = time.Duration(0) * time.Millisecond
				} else {
					duration = time.Duration(100) * time.Millisecond
				}
			} else if rsp.Result == int32(AE_TERM_UNMATCH) {
				if next_idx == 0 {
					log.Fatalf("peer:%s unmatch but we have reached the beginning", p.host)
				} else {
					// TODO(deyukong): too slow to find the common point, optimize
					next_idx -= 1
					if next_idx <= match_idx {
						log.Fatalf("peer:%s unmatch before match_idx:%d", p.host, match_idx)
					} else {
						log.Warnf("peer:%s nextidx:%d backoff by one", p.host, next_idx)
					}
				}
			} else if rsp.Result == int32(AE_RETRY) {
				log.Fatalf("peer:%s reply AE_RETRY", p.host)
			} else if rsp.Result == int32(AE_SMALL_TERM) {
				// nothing
			}
			translog_chan <- &AeWrapper{
				id:        p.host,
				res:       rsp,
				idx: match_idx,
                term: entries[len(entries)-1].Term,
			}
		}
	}
}

func (p *PeerServer) LeaderCron() {
	hb_term_chan := make(chan int64)
	tmp_stop_chan := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)
	go p.LeaderHeartBeatCron(hb_term_chan, tmp_stop_chan, &wg)

	translog_chan := make(chan *AeWrapper)
	for _, o := range p.cluster_info {
		wg.Add(1)
		go p.TransLog(o, translog_chan, tmp_stop_chan, &wg)
	}

	// TODO(deyukong): we must guarentee that master's term is invariant before the local channels terminates.
	// otherwise, the two goroutines will use the true-leader's(other than me) term to send hb or logentries
	// to followers. which in fact is a byzantine-situation.
	new_term := p.current_term
	var delayed_grantvote func() = nil

	// cleanup goroutines, be very careful about channel cyclic-dependency, alive-locks
	defer func() {
		close(tmp_stop_chan)
		// TODO(deyukong): stop translog channels
		wg_chan := make(chan int)
		go func() {
			wg.Wait()
			close(wg_chan)
		}()
		for {
			// no need to handle new_entry_pair and vote_pair, they are global and
			// every loop will handle these two pairs, but hb_term_chan and {???} are
			// local channels, we must strictly control their scopes, limit them in
			// leader-loop
			select {
			case tmp := <-hb_term_chan:
				log.Infof("ignore msg:%d from hb_term_chan since master stepping down", tmp)
			case <-wg_chan:
				log.Infof("hb channel and translog channels are all closed, leader stepdown finish")
				if new_term > p.current_term {
					p.ChangeState(pb.PeerState_Follower)
					p.UpdateTerm(new_term)
				}
				if delayed_grantvote != nil {
					delayed_grantvote()
				}
				return
			}
		}
	}()

	for {
		select {
		case <-p.close_chan:
			log.Infof("leader cron stops...")
			return
		case tmp := <-hb_term_chan:
			if tmp > p.current_term {
				log.Infof("leader term:%d got hb monotonic term:%d, turn follower", p.current_term, tmp)
				new_term = tmp
				return
			} else {
				log.Debugf("leader term:%d got hb:%d smaller", p.current_term, tmp)
			}
        case tmp := <-translog_chan:
			if tmp.res.Term > p.current_term {
				log.Infof("leader term:%d got[%s] translog monotonic term:%d, turn follower",
                    p.current_term, tmp.id, tmp.res.Term)
                if tmp.res.Result == int32(AE_OK) {
                    log.Fatalf("leader term:%d got[%s] invalid translog result:%v",
                        p.current_term, tmp.id, tmp.res)
                }
				new_term = tmp.res.Term
				return
			} else {
				log.Debugf("leader term:%d got[%s] hb:%d smaller", p.current_term, tmp.id, tmp.res.Term)
                p.ForwardCommitIndex()
			}
		case tmp := <-p.new_entry_pair.input:
			if p.current_term > tmp.Term {
				log.Infof("leader term:%d got entry with smaller term:%d", p.current_term, tmp.Term)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_SMALL_TERM),
					Msg:    fmt.Sprintf("req term:%d smaller than candidate:%d", tmp.Term, p.current_term),
					Term:   p.current_term,
				}
			} else if p.current_term == tmp.Term {
				log.Fatalf("BUG:leader term:%d meet appendentry:%v same term", p.current_term, tmp)
			} else {
				log.Infof("leader term:%d got entry monotonic term:%d, turn follower", p.current_term, tmp.Term)
				new_term = tmp.Term
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_RETRY),
					Msg:    "",
					Term:   new_term,
				}
				return
			}
		case tmp := <-p.vote_pair.input:
			if tmp.Term > new_term {
				new_term = tmp.Term
				// The grantVote process must be deferred to when local goroutines are all finished.
				delayed_grantvote = func() {
					ok, reason := p.GrantVote(tmp)
					result := &pb.RequestVoteRes{
						Header:      new(pb.ResHeader),
						VoteGranted: p.vote_for,
						Term:        p.current_term,
					}
					if !ok {
						log.Infof("leader term:%d got vote[%s:%d], not grant:%s",
							p.current_term, tmp.CandidateId, tmp.Term, reason)
					} else {
						log.Infof("leader term:%d got vote[%s:%d] and granted",
							p.current_term, tmp.CandidateId, tmp.Term)
					}
					p.vote_pair.output <- result
				}
				return
			} else {
				log.Infof("leader term:%d got smaller vote:%d from:%d, ignore",
					p.current_term, tmp.Term, tmp.CandidateId)
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					Term:        p.current_term,
				}
			}
		}
	}
}
