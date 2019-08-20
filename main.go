package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"

	"github.com/moooofly/dms-elector/pkg/util"
	"github.com/moooofly/dms-elector/server"

	"github.com/sirupsen/logrus"
)

const (
	modeSinglePoint = 1
	modeMasterSlave = 2
	modeCluster     = 3

	logLevelDebug = logrus.DebugLevel
	logLevelInfo  = logrus.InfoLevel
	logLevelWarn  = logrus.WarnLevel
)

const (
	versionFilePath = "./VERSION"
)

var reqSrvTCPHost string
var reqSrvUnixPath string
var persistentFilePath string
var mode int
var logFilePath string
var logLevel logrus.Level
var localEleSrvHost, remoteEleSrvHost string
var eleSrvConnTimeout uint
var seekVotePeriod uint
var seekVoteMaxTry uint
var pingPeriod uint
var leaderTimeout uint
var leaderBootstrapPeriod uint
var zkClusterHost []string
var zkLeaderDir string
var protectionPeriod uint

var version string

func init() {
	v, err := ioutil.ReadFile(versionFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read version file, will exit: %s\n", err.Error())
		os.Exit(-1)
	}
	version = string(v)
}

func parseConfigFile(path string) error {
	content, err := util.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	for _, line := range content {
		if len(line) == 1 || strings.Contains(line[0], "#") {
			// must be the comments
			continue
		}

		switch line[0] {
		case "local-tcp-request-server-host":
			reqSrvTCPHost = line[1]
		case "local-unix-request-server-path":
			reqSrvUnixPath = line[1]
		case "persistence-file-path":
			persistentFilePath = line[1]
		case "mode":
			switch line[1] {
			case "single-point":
				mode = modeSinglePoint
			case "master-slave":
				mode = modeMasterSlave
			case "cluster":
				mode = modeCluster
			default:
				fmt.Println("cannot parser elector mode")
				os.Exit(-1)
			}
		case "log-file-path":
			logFilePath = line[1]
		case "log-level":
			switch line[1] {
			case "debug":
				logLevel = logLevelDebug
			case "info":
				logLevel = logLevelInfo
			case "warn":
				logLevel = logLevelWarn
			default:
				fmt.Println("cannot parser log level")
				os.Exit(-1)
			}
		case "local-elector-server-host":
			localEleSrvHost = line[1]
		case "remote-elector-server-host":
			remoteEleSrvHost = line[1]
		case "remote-elector-server-connect-timeout":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			eleSrvConnTimeout = uint(n)
		case "seek-vote-period":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			seekVotePeriod = uint(n)
		case "seek-vote-max-try":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			seekVoteMaxTry = uint(n)
		case "ping-period":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			pingPeriod = uint(n)
		case "leader-timeout":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			leaderTimeout = uint(n)
		case "leader-bootstrap-period":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			leaderBootstrapPeriod = uint(n)
		case "zookeeper-cluster-host":
			zkClusterHost = append(zkClusterHost, line[1:]...)
		case "zookeeper-leader-dir":
			zkLeaderDir = line[1]
		case "protection-period":
			n, err := strconv.ParseUint(line[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}
			protectionPeriod = uint(n)
		}
	}

	return nil
}

func configLogger(f *os.File) {
	logrus.SetOutput(f)
	logrus.SetLevel(logLevel)
}

func printBanner() {
	logrus.Infof("======================= ELECTOR ========================")
	logrus.Infof("version: %s", version)
	logrus.Infof("starting elector using configure:")

	switch mode {
	case modeSinglePoint:
		logrus.Infof("mode: single point")
		logrus.Infof("local-request-server-host: %s", reqSrvTCPHost)
		logrus.Infof("persistence-file-path: %s", persistentFilePath)

	case modeMasterSlave:
		logrus.Infof("mode: master slave")
		logrus.Infof("local-request-server-host: %s", reqSrvTCPHost)
		logrus.Infof("persistence-file-path: %s", persistentFilePath)

		logrus.Infof("local-elector-server-host: %s", localEleSrvHost)
		logrus.Infof("remote-elector-server-host: %s", remoteEleSrvHost)
		logrus.Infof("remote-elector-server-connect-timeout: %d", eleSrvConnTimeout)
		logrus.Infof("seek-vote-period: %d", seekVotePeriod)
		logrus.Infof("seek-vote-max-try: %d", seekVoteMaxTry)
		logrus.Infof("ping-period: %d", pingPeriod)
		logrus.Infof("leader-timeout: %d", leaderTimeout)
		logrus.Infof("leader-bootstrap-period: %d", leaderBootstrapPeriod)

	case modeCluster:
		logrus.Infof("mode: cluster")
		logrus.Infof("local-request-server-host: %s", reqSrvTCPHost)
		logrus.Infof("persistence-file-path: %s", persistentFilePath)

		logrus.Infof("zookeeper-cluster-host: %s", strings.Join(zkClusterHost, " "))
		logrus.Infof("zookeeper-leader-dir: %s", zkLeaderDir)
		logrus.Infof("protection-period: %d", protectionPeriod)
	}
}

func main() {
	var err error
	var profilePath string
	var elector server.Elector

	if os.Args[1] == "--version" {
		fmt.Printf("elector version %s\n", version)
		return
	}

	// if do profile
	if len(os.Args) >= 4 && os.Args[2] == "-p" {
		profilePath = os.Args[3]
	}

	if err := parseConfigFile(os.Args[1]); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	// config logger
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer logFile.Close()
	configLogger(logFile)

	switch mode {
	case modeSinglePoint:
		elector = server.NewSinglePointElector(persistentFilePath, reqSrvTCPHost, reqSrvUnixPath)
	case modeMasterSlave:
		elector = server.NewMSElector(persistentFilePath, reqSrvTCPHost, reqSrvUnixPath, localEleSrvHost, remoteEleSrvHost,
			server.WithEleConnTimeout(eleSrvConnTimeout), server.WithSeekVotePeriod(seekVotePeriod),
			server.WithSeekVoteMaxTry(seekVoteMaxTry), server.WithPingPeriod(pingPeriod),
			server.WithLeaderTimeout(leaderTimeout), server.WithLeaderBootStrapPeriod(leaderBootstrapPeriod))
	case modeCluster:
		elector = server.NewClusterElector(persistentFilePath, reqSrvTCPHost, reqSrvUnixPath, zkClusterHost, zkLeaderDir,
			server.WithProtectionPeriod(protectionPeriod))
	}

	printBanner()

	if profilePath != "" {
		f, err := os.Create(profilePath)
		if err != nil {
			logrus.Infof("cannot create file %s\n", profilePath)
			os.Exit(-1)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		logrus.Infof("will do profile into %s", profilePath)
	}

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err = elector.Start()
		if err != nil {
			logrus.Infof(err.Error())
			close(stopCh)
		}
	}()

	<-stopCh
	elector.Stop()
	logrus.Infof("elector stopped")
}
