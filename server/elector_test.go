package server

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	cli "DMS-Elector/client"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var persistenceFilePath0, persistenceFilePath1, persistenceFilePath2 string

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		return
	}

	persistenceFilePath0 = cwd + "/election0.pst"
	persistenceFilePath1 = cwd + "/election1.pst"
	persistenceFilePath2 = cwd + "/election2.pst"

	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.DebugLevel)
}

////////////////////////////////////////////
// Single point elector tests
////////////////////////////////////////////

// It is meaningless to test a single point elector, but it's a good choice
// to test local request server of elector
func TestSinglePointElector_ReqSrv(t *testing.T) {
	host := "127.0.0.1:8293"
	path := "/tmp/elector.sock"

	e := newSinglePointElectorWithInfo(persistenceFilePath0, RoleLeader, host, path)
	e.Start()
	defer e.Stop()

	time.Sleep(time.Second)

	c := cli.NewClient(host, path, 5)
	c.Connect()
	defer c.Close()

	if role, err := c.Role(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, cli.RoleLeader, role)
		assert.Equal(t, RoleLeader, e.Role())
	}

	e.Abdicate()
	time.Sleep(time.Second)

	if role, err := c.Role(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, cli.RoleFollower, role)
		assert.Equal(t, RoleFollower, e.Role())
	}

	e.Promote()
	time.Sleep(time.Second)

	if role, err := c.Role(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, cli.RoleLeader, role)
		assert.Equal(t, RoleLeader, e.Role())
	}
}

// Test for user request server kick timeout user offline
func TestSinglePointElector_ReqSrvKickTimeoutConn(t *testing.T) {
	host := "127.0.0.1:8293"
	path := "/tmp/elector.sock"

	e := newSinglePointElectorWithInfo(persistenceFilePath0, RoleLeader, host, path)
	e.Start()
	defer e.Stop()

	time.Sleep(time.Second)

	c := cli.NewClient(host, path, 5)
	c.Connect()
	defer c.Close()

	if role, err := c.Role(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, cli.RoleLeader, role)
		assert.Equal(t, RoleLeader, e.Role())
	}

	time.Sleep(10 * time.Second)

	_, err := c.Abdicate()
	assert.NotNil(t, err)
}

////////////////////////////////////////////
// master-slave elector tests
////////////////////////////////////////////

/******** connection tests ********/
func TestMSElector_CannotConnect(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleCandidate, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithEleConnTimeout(2), WithSeekVoteMaxTry(5), WithSeekVotePeriod(1))

	go e0.Start()
	time.Sleep(10 * time.Second)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.NotEqual(t, connStateConnected, int(e0.eleConnHandler.connState))

	e0.Stop()
}

func TestMSElector_Reconnect(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithEleConnTimeout(5))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377",
		"127.0.0.1:1377", WithLeaderTimeout(3), WithEleConnTimeout(5))

	go e0.Start()
	go e1.Start()

	// election
	time.Sleep(5 * time.Second)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	// make leader down
	e0.Stop()
	fmt.Println("old leader down")
	time.Sleep(10 * time.Second)
	assert.Equal(t, RoleLeader, e1.Role())

	// restart old leader
	go e0.Start()
	fmt.Println("old leader restart")
	time.Sleep(10 * time.Second)
	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleLeader, e1.Role())

	e0.Stop()
	e1.Stop()
}

func TestMSElector_Reconnect_Leadership_Thrashing(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithLeaderTimeout(5), WithEleConnTimeout(8))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377",
		"127.0.0.1:1377", WithLeaderTimeout(5), WithEleConnTimeout(8))

	go e0.Start()
	go e1.Start()

	// election
	time.Sleep(3 * time.Second)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	// make leader down
	e0.Stop()
	fmt.Println("old leader down")
	time.Sleep(12 * time.Second)
	assert.Equal(t, RoleLeader, e1.Role())

	// restart old leader
	go e0.Start()
	fmt.Println("old leader restart")
	time.Sleep(5 * time.Second)
	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleLeader, e1.Role())

	time.Sleep(5 * time.Second)
	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleLeader, e1.Role())

	e0.Stop()
	e1.Stop()
}

/******** state machine tests ********/
func TestMSElector_ElectionCC(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleCandidate, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithLeaderTimeout(3))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleCandidate, 0, "", "", "127.0.0.1:2377",
		"127.0.0.1:1377", WithLeaderTimeout(3))

	go e0.Start()
	go e1.Start()

	time.Sleep(10 * time.Second)

	assert.NotEqual(t, RoleCandidate, e0.Role())
	assert.NotEqual(t, RoleCandidate, e1.Role())

	if e0.Role() == RoleLeader {
		assert.Equal(t, RoleFollower, e1.Role())
	} else {
		assert.Equal(t, RoleLeader, e1.Role())
	}

	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_ElectionCF(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleCandidate, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithSeekVotePeriod(1), WithSeekVoteMaxTry(5))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377",
		"127.0.0.1:1377", WithLeaderTimeout(5))

	go e0.Start()
	go e1.Start()

	time.Sleep(10 * time.Second)

	assert.NotEqual(t, RoleCandidate, e0.Role())
	assert.NotEqual(t, RoleCandidate, e1.Role())

	if e0.Role() == RoleLeader {
		assert.Equal(t, RoleFollower, e1.Role())
	} else {
		assert.Equal(t, RoleLeader, e1.Role())
	}

	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_ElectionCL(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleCandidate, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithSeekVotePeriod(1), WithSeekVoteMaxTry(5))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleLeader, 0, "", "", "127.0.0.1:2377", "127.0.0.1:1377")

	go e0.Start()
	go e1.Start()

	time.Sleep(10 * time.Second)

	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_ElectionFC(t *testing.T) {
	// equal to TestElectionCF
}

func TestMSElector_ElectionFF(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleFollower, 0, "", "", "127.0.0.1:1377",
		"127.0.0.1:2377", WithLeaderTimeout(5))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377",
		"127.0.0.1:1377", WithLeaderTimeout(5))

	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()

	time.Sleep(10 * time.Second)

	if e0.Role() == RoleLeader {
		assert.Equal(t, RoleFollower, e1.Role())
	} else {
		assert.Equal(t, RoleLeader, e1.Role())
	}

	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_ElectionFL(t *testing.T) {
	// tested by all cases
}

func TestMSElector_ElectionLC(t *testing.T) {
	// tested by TestElectionCL
}

func TestElectionLF(t *testing.T) {
	// tested by all cases
}

func TestMSElector_ElectionLLWithDiffEpoch(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	epoch0, epoch1 := rand.Uint64(), rand.Uint64()
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, epoch0, "", "", "127.0.0.1:1377", "127.0.0.1:2377")
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleLeader, epoch1, "", "", "127.0.0.1:2377", "127.0.0.1:1377")

	go e0.Start()
	go e1.Start()

	time.Sleep(10 * time.Second)

	if epoch0 > epoch1 {
		assert.Equal(t, RoleLeader, e0.Role())
	} else {
		assert.Equal(t, RoleFollower, e0.Role())
	}

	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_ElectionLLWithSameEpoch(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377", "127.0.0.1:2377")
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleLeader, 0, "", "", "127.0.0.1:2377", "127.0.0.1:1377")

	go e0.Start()
	time.Sleep(5 * time.Second)
	go e1.Start()

	time.Sleep(10 * time.Second)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
	assert.Equal(t, e0.epoch, e1.epoch)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_LeaderBootstrapTimeout(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377", "127.0.0.1:2377",
		WithLeaderBootStrapPeriod(3))

	go e0.Start()

	time.Sleep(2 * time.Second)
	assert.Equal(t, RoleUnstable, e0.Role())

	time.Sleep(5 * time.Second)
	assert.Equal(t, RoleLeader, e0.Role())

	e0.Stop()
}

func TestMSElector_LeaderBootstrapRecovery(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377", "127.0.0.1:2377",
		WithEleConnTimeout(5), WithLeaderBootStrapPeriod(100))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleLeader, 0, "", "", "127.0.0.1:2377", "127.0.0.1:1377",
		WithEleConnTimeout(5), WithLeaderBootStrapPeriod(100))
	go e0.Start()
	time.Sleep(2 * time.Second)
	assert.Equal(t, RoleUnstable, e0.Role())

	fmt.Println("another one online")
	go e1.Start()
	time.Sleep(10 * time.Second)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	e0.Stop()
	e1.Stop()
}

func TestMSElector_Abdicate(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377", "127.0.0.1:2377")
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377", "127.0.0.1:1377")

	go e0.Start()
	go e1.Start()

	time.Sleep(3 * time.Second)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
	assert.Equal(t, e0.epoch, e1.epoch)
	oldEpoch := e0.epoch

	e0.Abdicate()

	time.Sleep(5 * time.Second)

	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleLeader, e1.Role())
	assert.Equal(t, e0.epoch, e1.epoch)
	assert.Equal(t, e0.epoch, oldEpoch+1)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_Promote(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleLeader, 0, "", "", "127.0.0.1:1377", "127.0.0.1:2377")
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleFollower, 0, "", "", "127.0.0.1:2377", "127.0.0.1:1377")

	go e0.Start()
	go e1.Start()

	time.Sleep(3 * time.Second)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
	assert.Equal(t, e0.epoch, e1.epoch)
	oldEpoch := e0.epoch

	e1.Promote()

	time.Sleep(5 * time.Second)

	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleLeader, e1.Role())
	assert.Equal(t, e0.epoch, e1.epoch)
	assert.Equal(t, e0.epoch, oldEpoch+1)

	e0.Stop()
	e1.Stop()
}

func TestMSElector_MSReqSrv(t *testing.T) {
	e0 := newMSElectorWithInfo(persistenceFilePath0, RoleCandidate, 0, "127.0.0.1:8293", "/tmp/elector0.sock",
		"127.0.0.1:1377", "127.0.0.1:2377", WithLeaderTimeout(3))
	e1 := newMSElectorWithInfo(persistenceFilePath1, RoleCandidate, 0, "127.0.0.1:8294", "/tmp/elector1.sock",
		"127.0.0.1:2377", "127.0.0.1:1377", WithLeaderTimeout(3))

	go e0.Start()
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(10 * time.Second)

	cli0, cli1 := cli.NewClient("127.0.0.1:8293", "/tmp/elector0.sock", 5),
		cli.NewClient("127.0.0.1:8294", "/tmp/elector1.sock", 5)
	cli0.Connect()
	cli1.Connect()
	defer cli0.Close()
	defer cli1.Close()

	if e0.Role() == RoleLeader {
		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}
		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}

		cli0.Abdicate()
		time.Sleep(2 * time.Second)

		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}
		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}

		cli0.Promote()
		time.Sleep(2 * time.Second)
		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}
		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}
	} else if e0.Role() == RoleFollower {
		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}
		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}

		cli1.Abdicate()
		time.Sleep(2 * time.Second)

		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}
		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}

		cli1.Promote()
		time.Sleep(2 * time.Second)

		if r, err := cli1.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleLeader, r)
		}
		if r, err := cli0.Role(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, cli.RoleFollower, r)
		}
	} else {
		t.Fatal("should be a leader or follower")
	}
}

////////////////////////////////////////////
// cluster elector tests
////////////////////////////////////////////
/*
func TestZK(t *testing.T) {
	zkCli, _, err := zk.Connect(ZkCluster, time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer zkCli.Close()

	ok, _, ch, err := zkCli.ExistsW("/123")
	if err != nil {
		fmt.Println(ok, err)
		return
	}

	ok, _, ch2, err := zkCli.ExistsW("/123")
	if err != nil {
		fmt.Println(ok, err)
		return
	}

	ev := <-ch
	fmt.Println("from ch:", ev)

	ev = <-ch2
	fmt.Println("from ch2:", ev)
}
*/

var ZkCluster = []string{"127.0.0.1:2181"}
var LeaderDir = "/dir0/dir1/dir2/dir3/dir4"
var LeaderPath = LeaderDir + "/leader"

func prepare() {
	c, _, err := zk.Connect(ZkCluster, 100*time.Second)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer c.Close()

	exists, _, err := c.Exists(LeaderPath)
	if err != nil {
		log.Fatal("existed", err)
	}

	if exists {
		c.Delete(LeaderPath, -1)
	} else {
		path := ""
		dirs := strings.Split(LeaderDir, "/")

		for _, d := range dirs[1:] {
			path += "/" + d
			exist, _, err := c.Exists(path)
			if err != nil {
				log.Fatal(err)
			}

			if !exist {
				_, err := c.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}

}

func TestClusterElector_SingleCandidate(t *testing.T) {
	prepare()

	e := newClusterElectorWithInfo(persistenceFilePath0, RoleCandidate, "127.0.0.1:8293", "", ZkCluster, LeaderDir)
	go e.Start()
	defer e.Stop()

	time.Sleep(time.Second * 3)

	assert.Equal(t, RoleLeader, e.Role())
}

func TestClusterElector_TwoCandidates(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	go e0.Start()
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)

	assert.NotEqual(t, RoleCandidate, e0.Role())
	assert.NotEqual(t, RoleCandidate, e1.Role())

	var l, f *ClusterElector
	if e0.Role() == RoleLeader {
		l, f = e0, e1
		assert.Equal(t, RoleFollower, e1.Role())
	} else {
		l, f = e1, e0
		assert.Equal(t, RoleLeader, e1.Role())
	}

	// let leader down
	fmt.Println("let leader down")
	l.Stop()
	time.Sleep(time.Second * 5)
	assert.Equal(t, RoleLeader, f.Role())

	// let old leader restart
	fmt.Println("let old leader restart")
	go l.Start()

	time.Sleep(time.Second)
	assert.Equal(t, RoleLeader, f.Role())
	assert.Equal(t, RoleFollower, l.Role())
}

func TestClusterElector_TwoLeaders(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleLeader, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
}

func TestClusterElector_LongLongProtectionPeriod(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10000))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10000))
	go e0.Start()
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)

	assert.NotEqual(t, RoleCandidate, e0.Role())
	assert.NotEqual(t, RoleCandidate, e1.Role())

	var l, f *ClusterElector
	if e0.Role() == RoleLeader {
		l, f = e0, e1
		assert.Equal(t, RoleFollower, e1.Role())
	} else {
		l, f = e1, e0
		assert.Equal(t, RoleLeader, e1.Role())
	}

	// let leader down for 5 sec, should not change role
	fmt.Println("let leader down")
	l.Stop()
	time.Sleep(time.Second * 5)
	assert.Equal(t, RoleFollower, f.Role())

	// let old leader restart
	fmt.Println("let old leader restart")
	go l.Start()

	// should not change role
	time.Sleep(time.Second)
	assert.Equal(t, RoleFollower, f.Role())
	assert.Equal(t, RoleLeader, l.Role())
}

func TestClusterElector_ThreeCandidates(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	e2 := newClusterElectorWithInfo(persistenceFilePath2, RoleCandidate, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(2))
	go e0.Start()
	go e1.Start()
	go e2.Start()
	defer e0.Stop()
	defer e1.Stop()
	defer e2.Stop()

	time.Sleep(time.Second * 3)

	assert.NotEqual(t, RoleCandidate, e0.Role())
	assert.NotEqual(t, RoleCandidate, e1.Role())
	assert.NotEqual(t, RoleCandidate, e2.Role())

	var l, f0, f1 *ClusterElector
	if e0.Role() == RoleLeader {
		l, f0, f1 = e0, e1, e2
		assert.Equal(t, RoleFollower, f0.Role())
		assert.Equal(t, RoleFollower, f1.Role())
	}
	if e1.Role() == RoleLeader {
		l, f0, f1 = e1, e0, e2
		assert.Equal(t, RoleFollower, f0.Role())
		assert.Equal(t, RoleFollower, f1.Role())
	}
	if e2.Role() == RoleLeader {
		l, f0, f1 = e2, e0, e1
		assert.Equal(t, RoleFollower, f0.Role())
		assert.Equal(t, RoleFollower, f1.Role())
	}

	// let leader down
	var newL, newF *ClusterElector

	fmt.Println("let leader down")
	l.Stop()
	time.Sleep(time.Second * 5)
	if f0.Role() == RoleLeader {
		newL, newF = f0, f1
		assert.Equal(t, RoleFollower, f1.Role())
	} else {
		newL, newF = f1, f0
		assert.Equal(t, RoleLeader, f1.Role())
	}

	// let old leader restart
	fmt.Println("let old leader restart")
	go l.Start()
	time.Sleep(time.Second * 2)

	assert.Equal(t, RoleFollower, l.Role())
	assert.Equal(t, RoleLeader, newL.Role())
	assert.Equal(t, RoleFollower, newF.Role())
}

func TestClusterElector_Abdicate(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleFollower, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10))
	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	e0.Abdicate()

	time.Sleep(time.Second * 3)

	assert.Equal(t, RoleLeader, e1.Role())
	assert.Equal(t, RoleFollower, e0.Role())
}

func TestClusterElector_AbdicateThreeNodes(t *testing.T) {
	prepare()

	var l, f0, f1 *ClusterElector
	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, "", "", ZkCluster, LeaderDir)
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleFollower, "", "", ZkCluster, LeaderDir)
	e2 := newClusterElectorWithInfo(persistenceFilePath2, RoleFollower, "", "", ZkCluster, LeaderDir)
	l, f0, f1 = e0, e1, e2

	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	go e2.Start()
	defer e0.Stop()
	defer e1.Stop()
	defer e2.Stop()

	time.Sleep(time.Second * 3)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
	assert.Equal(t, RoleFollower, e2.Role())

	// let leader abdicate
	fmt.Println("let leader abdicate")
	e0.Abdicate()
	time.Sleep(time.Second * 5)

	assert.Equal(t, RoleFollower, e0.Role())
	f0 = e0
	if e1.Role() == RoleLeader {
		l, f1 = e1, e2
		assert.Equal(t, RoleFollower, e2.Role())
	} else {
		l, f1 = e2, e1
		assert.Equal(t, RoleFollower, e1.Role())
	}

	// make new leader abdicate again
	fmt.Println("let new leader abdicate again")
	l.Abdicate()
	time.Sleep(time.Second * 5)

	assert.Equal(t, RoleFollower, l.Role())
	if f0.Role() == RoleLeader {
		assert.Equal(t, RoleFollower, f1.Role())
	} else {
		assert.Equal(t, RoleFollower, f0.Role())
	}
}

func TestClusterElector_Promote(t *testing.T) {
	prepare()

	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10))
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleFollower, "", "", ZkCluster, LeaderDir, WithProtectionPeriod(10))
	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	e1.Promote()

	time.Sleep(time.Second * 3)

	assert.Equal(t, RoleLeader, e1.Role())
	assert.Equal(t, RoleFollower, e0.Role())
}

func TestClusterElector_PromoteThreeNodes(t *testing.T) {
	prepare()

	var l, f0, f1 *ClusterElector
	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, "", "", ZkCluster, LeaderDir)
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleFollower, "", "", ZkCluster, LeaderDir)
	e2 := newClusterElectorWithInfo(persistenceFilePath2, RoleFollower, "", "", ZkCluster, LeaderDir)
	l, f0, f1 = e0, e1, e2

	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	go e2.Start()
	defer e0.Stop()
	defer e1.Stop()
	defer e2.Stop()

	time.Sleep(time.Second * 3)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())
	assert.Equal(t, RoleFollower, e2.Role())

	// let one follower promote
	fmt.Println("let follower promote")
	f0.Promote()
	time.Sleep(time.Second * 5)

	assert.Equal(t, RoleLeader, f0.Role())
	l = f0
	assert.Equal(t, RoleFollower, e0.Role())
	assert.Equal(t, RoleFollower, e2.Role())
	f0, f1 = e0, e2

	// make another promotion
	fmt.Println("let another follower promote")
	f1.Promote()
	time.Sleep(time.Second * 5)

	assert.Equal(t, RoleLeader, f1.Role())
	assert.Equal(t, RoleFollower, l.Role())
	assert.Equal(t, RoleFollower, f0.Role())
}

func TestClusterElector_ReqServer(t *testing.T) {
	prepare()

	host0, host1 := "127.0.0.1:8394", "127.0.0.1:8395"
	path0, path1 := "/tmp/elector0.sock", "/tmp/elector1.sock"
	e0 := newClusterElectorWithInfo(persistenceFilePath0, RoleLeader, host0, path0, ZkCluster, LeaderDir)
	e1 := newClusterElectorWithInfo(persistenceFilePath1, RoleFollower, host1, path1, ZkCluster, LeaderDir)
	go e0.Start()
	time.Sleep(time.Second)
	go e1.Start()
	defer e0.Stop()
	defer e1.Stop()

	time.Sleep(time.Second * 3)
	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, RoleFollower, e1.Role())

	cli0, cli1 := cli.NewClient(host0, path0, 5), cli.NewClient(host1, path1, 5)
	cli0.Connect()
	cli1.Connect()
	defer cli0.Close()
	defer cli1.Close()

	r0, err := cli0.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}
	r1, err := cli1.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}

	assert.Equal(t, cli.RoleLeader, r0)
	assert.Equal(t, cli.RoleFollower, r1)

	cli0.Abdicate()
	time.Sleep(time.Second * 3)

	r0, err = cli0.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}
	r1, err = cli1.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}
	assert.Equal(t, cli.RoleLeader, r1)
	assert.Equal(t, cli.RoleFollower, r0)

	cli0.Promote()
	time.Sleep(time.Second * 3)

	r0, err = cli0.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}
	r1, err = cli1.Role()
	if err != nil {
		assert.FailNowf(t, "err: %s", err.Error())
	}
	assert.Equal(t, cli.RoleLeader, r0)
	assert.Equal(t, cli.RoleFollower, r1)
}

////////////////////////////////////////////
// Persistence tests
////////////////////////////////////////////
func TestPersistence(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := cwd + "/election.pst"

	roles := []Role{RoleLeader, RoleFollower, RoleCandidate}
	for i := 0; i < 100; i++ {
		rand.Seed(time.Now().UnixNano())
		role := roles[rand.Intn(len(roles))]
		epoch := rand.Uint64()

		writePersistenceFile(path, role, epoch)
		getR, getE := readPersistenceFile(path)

		assert.Equal(t, role, getR)
		assert.Equal(t, epoch, getE)
	}
}

func TestElectorPersistence(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := cwd + "/election.pst"

	// start without election.pst
	os.Remove(path)

	e0 := NewMSElector(path, "", "", "127.0.0.1:1377", "127.0.0.1:2377",
		WithSeekVoteMaxTry(2), WithSeekVotePeriod(1))
	assert.Equal(t, RoleCandidate, e0.Role())
	assert.Equal(t, uint64(0), e0.epoch)

	go e0.Start()
	time.Sleep(5 * time.Second)

	assert.Equal(t, RoleLeader, e0.Role())
	assert.Equal(t, uint64(1), e0.epoch)

	r, e := readPersistenceFile(path)
	assert.Equal(t, e0.Role(), r)
	assert.Equal(t, e0.epoch, e)

	e0.Stop()

	r, e = readPersistenceFile(path)
	assert.Equal(t, e0.Role(), r)
	assert.Equal(t, e0.epoch, e)

	// start with
	e1 := NewMSElector(path, "", "", "127.0.0.1:1377", "127.0.0.1:2377")

	rr, ee := readPersistenceFile(path)
	assert.Equal(t, r, rr)
	assert.Equal(t, e, ee)

	assert.Equal(t, rr, e1.Role())
	assert.Equal(t, ee, e1.epoch)

	go e1.Start()
	time.Sleep(5 * time.Second)

	assert.Equal(t, rr, e1.Role())
	assert.Equal(t, ee, e1.epoch)

	e1.Stop()
}
