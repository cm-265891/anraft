package main

import (
	"anraft/peer"
	"anraft/proto/peer_proto"
	"anraft/utils"
	"flag"
	"fmt"
	"github.com/ngaut/log"
	"github.com/sevlyar/go-daemon"
	"github.com/tsuru/config"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var configFile = flag.String("conf", "conf/peer.conf", "configure file for peer module")

func main() {
	flag.Parse()
	cntxt := &daemon.Context{}
	d, err := cntxt.Reborn()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	if d != nil {
		os.Exit(0)
	}
	defer cntxt.Release()

	// now we are in fork child process
	if err := Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newPeerServer(port int) (*peer.PeerServer, error) {
	s := &peer.PeerServer{}
	if err := s.Init(port); err != nil {
		return nil, err
	}
	return s, nil
}

func Main() error {
	err := config.ReadConfigFile(*configFile)
	if err != nil {
		return err
	}

	port := utils.GetConfigIntOrDefault("peer:port", 6000)
	logLevel := utils.GetConfigStringOrDefault("logLevel", "debug")
	logPath := utils.GetConfigStringOrDefault("logPath", "./log/")
	// pidFile := utils.GetConfigStringOrDefault("pidFile", "./data/peer.pid")

	// redirect the stdout and stderr
	stdFile, _ := os.OpenFile(fmt.Sprintf("%s/stdlog-%d", logPath, time.Now().Unix()),
		os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	syscall.Dup2(int(stdFile.Fd()), 1)
	syscall.Dup2(int(stdFile.Fd()), 2)

	log.SetLevelByString(logLevel)
	log.SetRotateByHour()
	log.SetHighlighting(false)
	log.SetOutputByName(fmt.Sprintf("%s/peer.log", logPath))

	listener, err1 := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err1 != nil {
		log.Errorf("listen failed: %v", err1)
		return err1
	}
	var opts []grpc.ServerOption
	grpc_svr := grpc.NewServer(opts...)
	peer_svr, err := newPeerServer(port)
	if err != nil {
		log.Errorf("new server failed:%v", err)
		return err
	}
	peer_proto.RegisterPeerServer(grpc_svr, peer_svr)
	go grpc_svr.Serve(listener)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	log.Infof("Server Got signal [%d] to exit.", sig)
	signal.Stop(sc)
	grpc_svr.Stop()
	return nil
}
