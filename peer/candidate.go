package peer

import (
	pb "anraft/proto/peer_proto"
	"fmt"
	"github.com/ngaut/log"
	context "golang.org/x/net/context"
	"math/rand"
	"sync"
	"time"
)

// NOTE(deyukong): this is run in a temp thread, never touch shared states
func requestVote(p *PeerInfo, term int64, timeout time.Duration,
	last_entry *pb.LogEntry, channel chan *pb.RequestVoteRes, wg *sync.WaitGroup) {
	defer wg.Done()
	req := new(pb.RequestVoteReq)
	req.Header = new(pb.ReqHeader)
	req.Term = term
	req.CandidateId = p.id
	req.LastLogIndex = -1
	req.LastLogTerm = -1
	if last_entry != nil {
		req.LastLogIndex = last_entry.Index
		req.LastLogTerm = last_entry.Term
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	rsp, err := p.client.RequestVote(ctx, req)
	if err != nil {
		log.Errorf("request vote:%v failed:%v", req, err)
		return
	}
	channel <- rsp
}

func (p *PeerServer) Elect() {
	// TODO(deyukong): the rule to modify state
	p.ChangeState(pb.PeerState_Candidate)
	vote_chan := make(chan *pb.RequestVoteRes, len(p.cluster_info))

	new_term := p.VoteForSelf(vote_chan)
	log.Infof("begin a new election with new_term:%d", new_term)
	var wg sync.WaitGroup
	wchan := make(chan int)
	last_entry, err := p.store.GetLastLogEntry()
	if err != nil {
		log.Fatalf("get last entry failed:%v", err)
	}
	for _, o := range p.cluster_info {
		if o.id == p.id {
			continue
		}
		wg.Add(1)
		go requestVote(o, new_term, p.election_timeout, last_entry, vote_chan, &wg)
	}
	go func() {
		wg.Wait()
		close(wchan)
		close(vote_chan)
	}()

	for {
		select {
		case <-wchan:
			log.Infof("elect term:%d collect vote done", new_term)
			goto WAIT_ELECT_FOR_END
		case tmp := <-p.new_entry_pair.input:
			if new_term > tmp.Term {
				log.Infof("elect term:%d got entry with smaller term:%d, keep wait", new_term, tmp.Term)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_SMALL_TERM),
					Msg:    fmt.Sprintf("reqterm:%d smaller than candidate:%d", tmp.Term, new_term),
					Term:   new_term,
				}
			} else {
				log.Infof("elect term:%d got entry monotonic term:%d, turn to follower", new_term, tmp.Term)
				p.UpdateTerm(tmp.Term)
				p.ChangeState(pb.PeerState_Follower)
				p.new_entry_pair.output <- &pb.AppendEntriesRes{
					Header: new(pb.ResHeader),
					Result: int32(AE_RETRY),
					Msg:    "",
					Term:   p.current_term,
				}
				return
			}
		case tmp := <-p.vote_pair.input:
			ok, reason := p.GrantVote(tmp)
			if !ok {
				log.Infof("elect term:%d got vote, now_term:%d not grant:%s", new_term, p.current_term, reason)
			} else {
				log.Infof("elect term:%d got vote and granted, current_term:%d", new_term, p.current_term)
			}
			p.vote_pair.output <- &pb.RequestVoteRes{
				Header:      new(pb.ResHeader),
				VoteGranted: p.vote_for,
				// no matter vote granted or not, it is still guaranteed that if other's term is greater,
				// ours is also updated to other's
				Term: p.current_term,
			}
			if p.current_term > new_term {
				log.Infof("after grantVote, cur_term:%d, old_term:%d, turn follower", p.current_term, new_term)
				p.ChangeState(pb.PeerState_Follower)
				return
			}
		case <-p.closer.HasBeenClosed():
			return
		}
	}

WAIT_ELECT_FOR_END:

	max_term := new_term
	same_term_map := make(map[string]int)
	for o := range vote_chan {
		if o.Term < new_term {
			log.Fatalf("RequestVote rep see term:%d less than mine:%d", o.Term, new_term)
		}
		if o.Term > max_term {
			max_term = o.Term
		}
		if o.Term == new_term {
			if _, ok := same_term_map[o.VoteGranted]; ok {
				same_term_map[o.VoteGranted] += 1
			} else {
				same_term_map[o.VoteGranted] = 1
			}
		}
	}

	// see paper chaptor-5, rules for all servers
	// if rpc request or response contains term T > currentTerm,
	// update currentTerm to T and conver to follower
	if max_term > new_term {
		p.UpdateTerm(max_term)
		p.ChangeState(pb.PeerState_Follower)
		return
	}

	majority_id := ""
	for id, num := range same_term_map {
		if num >= len(p.cluster_info)/2+1 {
			if majority_id == "" {
				majority_id = id
			} else {
				log.Fatalf("term:%d have two ids:%s,%s got major votes", new_term, majority_id, id)
			}
		}
	}
	log.Infof("same_term_map:%+v", same_term_map)

	if majority_id == "" {
		// [0, 2) * p.election_timeout
		nanocount := int64(p.election_timeout / time.Nanosecond)
		sleep := time.Duration(int64(2*rand.Float64()*float64(nanocount))) * time.Nanosecond
		log.Infof("elect draw, sleep for:%v and retry", sleep)
		tchan := time.After(sleep)
		for {
			select {
			case <-tchan:
				p.Elect()
				return
			case tmp := <-p.new_entry_pair.input:
				if new_term > tmp.Term {
					log.Infof("elect term:%d got entry with smaller term:%d, keep wait", new_term, tmp.Term)
					p.new_entry_pair.output <- &pb.AppendEntriesRes{
						Header: new(pb.ResHeader),
						Result: int32(AE_SMALL_TERM),
						Msg:    fmt.Sprintf("reqterm:%d smaller than candidate:%d", tmp.Term, new_term),
						Term:   new_term,
					}
				} else {
					log.Infof("elect term:%d got entry with monotonic term:%d, turn follower", new_term, tmp.Term)
					p.UpdateTerm(tmp.Term)
					p.ChangeState(pb.PeerState_Follower)
					p.new_entry_pair.output <- &pb.AppendEntriesRes{
						Header: new(pb.ResHeader),
						Result: int32(AE_RETRY),
						Msg:    "",
						Term:   p.current_term,
					}
					return
				}
			case tmp := <-p.vote_pair.input:
				ok, reason := p.GrantVote(tmp)
				if !ok {
					log.Infof("elect term:%d got vote, now_term:%d not grant:%s", new_term, p.current_term, reason)
				} else {
					log.Infof("elect term:%d got vote and granted, current_term:%d", new_term, p.current_term)
				}
				p.vote_pair.output <- &pb.RequestVoteRes{
					Header:      new(pb.ResHeader),
					VoteGranted: p.vote_for,
					// no matter vote granted or not, it is still guaranteed that if other's term is greater,
					// ours is also updated to other's
					Term: p.current_term,
				}
				if p.current_term > new_term {
					log.Infof("after grantVote, cur_term:%d, old_term:%d, turn follower", p.current_term, new_term)
					p.ChangeState(pb.PeerState_Follower)
					return
				}
			case <-p.closer.HasBeenClosed():
				return
			}
		}
	} else if majority_id == p.id {
		p.ChangeState(pb.PeerState_Leader)
	} else {
		p.ChangeState(pb.PeerState_Follower)
	}
}

func (p *PeerServer) VoteForSelf(channel chan *pb.RequestVoteRes) int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if err := p.updateTermInLock(p.current_term + 1); err != nil {
		log.Fatalf("incrTermInLock failed:%v", err)
	}
	if err := p.voteInLock(p.id); err != nil {
		log.Fatalf("voteInLock failed:%v", err)
	}
	rsp := new(pb.RequestVoteRes)
	rsp.Header = new(pb.ResHeader)
	rsp.Term = p.current_term
	rsp.VoteGranted = p.id
	channel <- rsp
	return p.current_term
}
