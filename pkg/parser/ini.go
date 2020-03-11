package parser

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

var (
	cfg *ini.File

	GlobalSetting      = &global{}
	SinglePointSetting = &single{}
	MasterSlaveSetting = &ms{}
	ClusterSetting     = &cluster{}
)

// [global] section in .ini
type global struct {
	TcpHost   string `ini:"local-tcp-request-server-host"`
	UnixHost  string `ini:"local-unix-request-server-path"`
	StateFile string `ini:"persistence-file-path"`
	LogFile   string `ini:"log-file"`
	LogLevel  string `ini:"log-level"`
	Mode      string `ini:"mode"`
}

// [single-point] section in .ini
type single struct {
}

// [master-slave] section in .ini
type ms struct {
	Local         string `ini:"local-elector-server-host"`
	Remote        string `ini:"remote-elector-server-host"`
	RetryPeriod   uint   `ini:"retry-period"`
	PingPeriod    uint   `ini:"ping-period"`
	LeaderTimeout uint   `ini:"leader-timeout"`
}

// [cluster] section in .ini
type cluster struct {
	ZkClusterHosts   string `ini:"zookeeper-cluster-host"`
	ZkLeaderDir      string `ini:"zookeeper-leader-dir"`
	ProtectionPeriod int    `ini:"protection-period"`
}

func Load(confPath string) {
	cf := fmt.Sprintf("%s/elector.ini", confPath)

	var err error
	cfg, err = ini.Load(cf)
	if err != nil {
		logrus.Fatalf("Fail to parse '%s': %v", cf, err)
	}

	mapTo("global", GlobalSetting)

	switch GlobalSetting.Mode {
	case "single-point":
		mapTo("single-point", SinglePointSetting)
	case "master-slave":
		mapTo("master-slave", MasterSlaveSetting)
	case "cluster":
		mapTo("cluster", ClusterSetting)
	default:
		logrus.Fatal("mode '%v' dose not match any of [single-point|master-slave|cluster].", GlobalSetting.Mode)
	}
	logrus.Infof("mode setting => [%s]", GlobalSetting.Mode)
}

func mapTo(section string, v interface{}) {
	err := cfg.Section(section).MapTo(v)
	if err != nil {
		logrus.Fatalf("mapto err: %v", err)
	}
}
