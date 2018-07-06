package peer

import (
	pb "anraft/proto/peer_proto"
	context "golang.org/x/net/context"
    "github.com/ngaut/log"
)

type PeerInfo struct {
    host string
}

type PeerServer struct {
    host string
    cluster_info []*PeerInfo
    state pb.PeerState
    stop_chan chan int
    /* the name of election_timeout is from 5.2 */
    /* if a follower receives on communication over a period of time */
    /* called the election timeout, then ...... */
    election_timeout time.Duration
}

// TODO(deyukong): we should make some guarentees about the modifications to state
// who can change the state?

func (p *PeerServer) Init(host string, hosts []string) error {
    p.host = host
    p.cluster_info = make([]*PeerInfo)
    for _, o := range hosts {
        p.cluster_info = append(p.cluster_info, o)
    }
    /* see 5.2 leader election */
    /* when servers start up, they begin as followers */
    p.state = pb.PeerState_Follower
    go p.FollowerCron()
	return nil
}

func (p *PeerServer) HeartBeat(context.Context, *pb.HeartBeatReq) (*pb.HeartBeatRes, error) {
	return nil, nil
}

func (p *PeerServer) ElecTimeout() {
    if p.state != pb.PeerState_Follower {
        log.Panicf("ElecTimeout in wrong state:%v", p.state)
    }
    go p.Elect()
}

func (p *PeerServer) FollowerCron() {
    for {
        select {
        case <- p.stop_chan:
            log.Infof("follower cron stops...")
            return
        case <- time.After(p.election_timeout):
            p.ElecTimeout()
            return
        case <- p.reset_elec_chan:
            log.Debugf("reset election chan...")
            break
        }
    }
}

func (p *PeerServer) Elect() {
    // TODO(deyukong): the rule to modify state
    p.state = pb.PeerState_Candidate
}
