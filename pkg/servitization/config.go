package servitization

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime/debug"
	"time"

	"github.com/moooofly/dms-elector/pkg/parser"
	"github.com/moooofly/dms-elector/pkg/version"
	"github.com/moooofly/dms-elector/server"
	"github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var el server.Elector

const (
	logLevelDebug = logrus.DebugLevel
	logLevelInfo  = logrus.InfoLevel
	logLevelWarn  = logrus.WarnLevel
)

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

func configLogger(f *os.File) {
	logrus.SetOutput(f)
	logrus.SetLevel(logLevel)
}

func printBanner() {
	logrus.Infof("======================= ELECTOR ========================")
	logrus.Infof("version: %s", version.Version)
	logrus.Infof("======================= ======= ========================")
}

// CLI facilities
var (
	app *kingpin.Application
	cmd *exec.Cmd
)

var isDebug *bool

func Init() (err error) {

	// TODO: 定制 logrus 日志格式

	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)

	app = kingpin.New("elector", "This is a component of dms called elector.")
	app.Author("moooofly").Version(version.Version)

	// global settings
	isDebug = app.Flag("debug", "debug log output").Default("false").Bool()
	daemon := app.Flag("daemon", "run elector in background").Default("false").Bool()
	forever := app.Flag("forever", "run elector in forever, fail and retry").Default("false").Bool()
	logfile := app.Flag("log", "log file path").Default("").String()
	nolog := app.Flag("nolog", "turn off logging").Default("false").Bool()

	// ini 配置解析
	parser.Load()

	if *isDebug {
		startProfiling()
	}

	if *nolog {
		logrus.SetOutput(ioutil.Discard)
	} else if *logfile != "" {
		f, e := os.OpenFile(*logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if e != nil {
			logrus.Fatal(e)
		}
		logrus.SetOutput(f)
	}
	if *daemon {
		args := []string{}
		for _, arg := range os.Args[1:] {
			if arg != "--daemon" {
				args = append(args, arg)
			}
		}
		cmd = exec.Command(os.Args[0], args...)
		cmd.Start()
		f := ""
		if *forever {
			f = "forever "
		}
		logrus.Printf("%s%s [PID] %d running...\n", f, os.Args[0], cmd.Process.Pid)
		os.Exit(0)
	}
	if *forever {
		args := []string{}
		for _, arg := range os.Args[1:] {
			if arg != "--forever" {
				args = append(args, arg)
			}
		}
		go func() {
			defer func() {
				if e := recover(); e != nil {
					fmt.Printf("crashed, err: %s\nstack:%s", e, string(debug.Stack()))
				}
			}()
			for {
				if cmd != nil {
					cmd.Process.Kill()
					time.Sleep(time.Second * 5)
				}
				cmd = exec.Command(os.Args[0], args...)
				cmdReaderStderr, err := cmd.StderrPipe()
				if err != nil {
					logrus.Printf("ERR: %s, restarting...\n", err)
					continue
				}
				cmdReader, err := cmd.StdoutPipe()
				if err != nil {
					logrus.Printf("ERR: %s, restarting...\n", err)
					continue
				}
				scanner := bufio.NewScanner(cmdReader)
				scannerStdErr := bufio.NewScanner(cmdReaderStderr)
				go func() {
					defer func() {
						if e := recover(); e != nil {
							fmt.Printf("crashed, err: %s\nstack:%s", e, string(debug.Stack()))
						}
					}()
					for scanner.Scan() {
						fmt.Println(scanner.Text())
					}
				}()
				go func() {
					defer func() {
						if e := recover(); e != nil {
							fmt.Printf("crashed, err: %s\nstack:%s", e, string(debug.Stack()))
						}
					}()
					for scannerStdErr.Scan() {
						fmt.Println(scannerStdErr.Text())
					}
				}()
				if err := cmd.Start(); err != nil {
					logrus.Printf("ERR: %s, restarting...\n", err)
					continue
				}
				pid := cmd.Process.Pid
				logrus.Printf("worker %s [PID] %d running...\n", os.Args[0], pid)
				if err := cmd.Wait(); err != nil {
					logrus.Printf("ERR: %s, restarting...", err)
					continue
				}
				logrus.Printf("worker %s [PID] %d unexpected exited, restarting...\n", os.Args[0], pid)
			}
		}()
		return
	}
	if *logfile == "" {
		if *isDebug {
			logrus.Println("[profiling] cpu profiling save to file: cpu.prof")
			logrus.Println("[profiling] memory profiling save to file: memory.prof")
			logrus.Println("[profiling] block profiling save to file: block.prof")
			logrus.Println("[profiling] goroutine profiling save to file: goroutine.prof")
			logrus.Println("[profiling] threadcreate profiling save to file: threadcreate.prof")
		}
	}

	switch parser.GlobalSetting.Mode {
	case "single-point":
		el = server.NewSinglePoint(
			parser.GlobalSetting.StateFile,
			parser.GlobalSetting.TcpHost,
			parser.GlobalSetting.UnixHost,
		)
	case "master-slave":
		el = server.NewMasterSlave(
			parser.GlobalSetting.StateFile,
			parser.GlobalSetting.TcpHost,
			parser.GlobalSetting.UnixHost,

			localEleSrvHost,
			remoteEleSrvHost,
			server.WithEleConnTimeout(eleSrvConnTimeout),
			server.WithSeekVotePeriod(seekVotePeriod),
			server.WithSeekVoteMaxTry(seekVoteMaxTry),
			server.WithPingPeriod(pingPeriod),
			server.WithLeaderTimeout(leaderTimeout),
			server.WithLeaderBootStrapPeriod(leaderBootstrapPeriod),
		)
	case "cluster":
		el = server.NewClusterElector(
			parser.GlobalSetting.StateFile,
			parser.GlobalSetting.TcpHost,
			parser.GlobalSetting.UnixHost,

			zkClusterHost,
			zkLeaderDir,
			server.WithProtectionPeriod(protectionPeriod),
		)
	}

	printBanner()

	go func() {
		if err := el.Start(); err != nil {
			logrus.Fatal(err)
		}
	}()

	return
}

func Teardown() {
	logrus.Info("elector stopping...")

	if cmd != nil {
		logrus.Infof("clean process %d", cmd.Process.Pid)
		cmd.Process.Kill()
	} else {
		el.Stop()
	}
	if *isDebug {
		saveProfiling()
	}
}
