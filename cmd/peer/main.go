package main

import (
	"anraft/peer"
	"anraft/proto/peer_proto"
	"anraft/storage/badger"
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
	"strings"
	"syscall"
	"time"
)

var configExample = `
configfile is a yaml file, refer to the example below:
logLevel: debug
logPath: ./log
peer:
    host: 10.172.12.31
    id: raft0
    cluster:
        raft0/10.172.12.31
        raft1/10.172.12.32
        raft2/10.172.12.33
        raft3/10.172.12.34
        raft4/10.172.12.35
`

var configFile = flag.String("conf", "conf/peer.conf", configExample)

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

func Main() error {
	err := config.ReadConfigFile(*configFile)
	if err != nil {
		return err
	}

	logLevel := utils.GetConfigStringOrDefault("logLevel", "debug")
	logPath := utils.GetConfigStringOrDefault("logPath", "./log/")

	// redirect the stdout and stderr
	stdFile, _ := os.OpenFile(fmt.Sprintf("%s/stdlog-%d", logPath, time.Now().Unix()),
		os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	syscall.Dup2(int(stdFile.Fd()), 1)
	syscall.Dup2(int(stdFile.Fd()), 2)

	log.SetLevelByString(logLevel)
	log.SetRotateByHour()
	log.SetHighlighting(false)
	log.SetOutputByName(fmt.Sprintf("%s/peer.log", logPath))

	host := utils.GetConfigStringOrDefault("peer:host", "")
	if host == "" {
		log.Errorf("host not configured")
		return fmt.Errorf("host not configured")
	}

	id := utils.GetConfigStringOrDefault("peer:id", "")
	if id == "" {
		log.Errorf("id not configured")
		return fmt.Errorf("id not configured")
	}

	cluster_lst, err := config.GetList("peer:cluster")
	if err != nil {
		log.Errorf("get peer:cluster failed:%v", err)
		return err
	}
	cluster_map := make(map[string]string)
	for _, c := range cluster_lst {
		c_lst := strings.Split(c, "/")
		if len(c_lst) != 2 {
			log.Errorf("bad peer format:%s", c)
			return fmt.Errorf("bad peer format:%s", c)
		}
		if _, ok := cluster_map[c_lst[0]]; ok {
			log.Errorf("duplicated peerid:%s peer[%s|%s]", c_lst[0], c_lst[1], cluster_map[c_lst[0]])
			return fmt.Errorf("duplicated peer")
		}
		cluster_map[c_lst[0]] = c_lst[1]
	}

	storage := utils.GetConfigStringOrDefault("raftlog:storage:engine", "badger")
	if storage != "badger" {
		log.Errorf("raftlog storage plugin currently supports badger only")
		return fmt.Errorf("raftlog storage plugin currently supports badger only")
	}
	store_dir := utils.GetConfigStringOrDefault("raftlog:storage:engine:dir", "./")
	engine := &badger.BadgerEngine{}
	if err := engine.Init(store_dir); err != nil {
		log.Errorf("init storage with dir:%s failed:%v", store_dir, err)
		return err
	}

	elect_timeout := utils.GetConfigIntOrDefault("raftlog:election_timeout_ms", 500)
	elect_timeout_dur := time.Duration(elect_timeout) * time.Millisecond

	listener, err1 := net.Listen("tcp", host)
	if err1 != nil {
		log.Errorf("listen failed: %v", err1)
		return err1
	}
	var opts []grpc.ServerOption
	grpc_svr := grpc.NewServer(opts...)

	peer_svr := &peer.PeerServer{}
	if err := peer_svr.Init(id, host, cluster_map, engine, elect_timeout_dur); err != nil {
		log.Errorf("peer server init failed:%v", err)
		return err
	}
	peer_proto.RegisterPeerServer(grpc_svr, peer_svr)
    peer_svr.Start()
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
    peer_svr.Stop()
	return nil
}
