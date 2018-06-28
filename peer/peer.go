package peer

import (
	pb "anraft/proto/peer_proto"
	context "golang.org/x/net/context"
)

type PeerServer struct {
	port int
}

func (p *PeerServer) Init(port int) error {
	p.port = port
	return nil
}

func (p *PeerServer) HeartBeat(context.Context, *pb.HeartBeatReq) (*pb.HeartBeatRes, error) {
	return nil, nil
}
