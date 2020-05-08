package parser

import (
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
	RoleServiceIp       string `ini:"role-service-ip"`
	RoleServicePort     string `ini:"role-service-port"`
	RoleServiceUnixPath string `ini:"role-service-unix-path"`
	StateFile           string `ini:"elector-state-file"`
	LogFile             string `ini:"log-file"`
	LogLevel            string `ini:"log-level"`
	Mode                string `ini:"mode"`
}

// [single-point] section in .ini
type single struct {
}

// [master-slave] section in .ini
type ms struct {
	LocalEleIp    string `ini:"local-elector-ip"`
	LocalElePort  string `ini:"local-elector-port"`
	RemoteEleIp   string `ini:"remote-elector-ip"`
	RemoteElePort string `ini:"remote-elector-port"`
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

func Load(confFile string) {
	var err error
	cfg, err = ini.LoadSources(ini.LoadOptions{
		IgnoreInlineComment: true,
	}, confFile)
	if err != nil {
		logrus.Fatalf("Fail to parse '%s': %v", confFile, err)
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
		logrus.Fatalf("mode '%v' dose not match any of [single-point|master-slave|cluster].", GlobalSetting.Mode)
	}
	//logrus.Infof("mode setting => [%s]", GlobalSetting.Mode)
}

func mapTo(section string, v interface{}) {
	err := cfg.Section(section).MapTo(v)
	if err != nil {
		logrus.Fatalf("mapto err: %v", err)
	}
}
