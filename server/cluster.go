package server

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

/********** Cluster mode elector **********/
type request uint

const (
	reqAbdicate = request(0)
	reqPromote  = request(1)
)

const (
	zkLeaderNodeName   = "leader"
	zkAbdicateFlagName = "abdicate"
)

// ClusterElector is used in cluster mode
type ClusterElector struct {
	id    uint64
	role  Role
	epoch uint64

	state electorState // running state

	zkLeaderDir    string // leader dir in zk
	zkLeaderPath   string // leader path in zk
	zkAbdicatePath string // leader abdicate path in zk

	zkHost        []string     // (cluster) host(s)
	zkConn        *zk.Conn     // conn
	zkConnHandler *connHandler // connection handler of zookeeper

	reqSrv *requestServer // user request server
	reqCh  chan request   // user request channel

	options electorOptions

	stopCh chan struct{} // stop signal channel

	path string // persistence file path
}

func doConnectRemoteZkWrapper(args ...interface{}) error {
	e := args[0].(*ClusterElector)
	return e.doConnectZkSrv()
}

func doCloseRemoteZkWrapper(args ...interface{}) error {
	e := args[0].(*ClusterElector)
	return e.doCloseZkSrv()
}

// NewClusterElector is the constructor of ClusterElector
func NewClusterElector(path, reqSrvHost, reqSrvPath string, zkHost []string, zkLeaderDir string, opts ...electorOption) *ClusterElector {
	role, _ := loadState(path)
	return newClusterElectorWithInfo(path, role, reqSrvHost, reqSrvPath, zkHost, zkLeaderDir, opts...)
}

func newClusterElectorWithInfo(path string, role Role, reqSrvHost, reqSrvPath string, zkHost []string, zkLeaderDir string, opts ...electorOption) *ClusterElector {
	var e ClusterElector

	e.id = rand.Uint64()
	e.role = role
	e.epoch = 0

	e.zkLeaderDir = zkLeaderDir
	e.zkLeaderPath = zkLeaderDir + "/" + zkLeaderNodeName
	e.zkAbdicatePath = zkLeaderDir + "/" + zkAbdicateFlagName
	e.zkHost = zkHost
	e.reqSrv = newRequestServer(reqSrvHost, reqSrvPath, &e)
	e.path = path

	// defaults
	e.options.protectionPeriod = 30

	for _, o := range opts {
		o(&e.options)
	}

	e.zkConnHandler = &connHandler{connState: connStateDisconnect, connectF: nil, closeF: nil}
	e.zkConnHandler.registerConnFunc(doConnectRemoteZkWrapper)
	e.zkConnHandler.registerCloseFunc(doCloseRemoteZkWrapper)

	return &e
}

// Info the elector
func (e *ClusterElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start the elector
func (e *ClusterElector) Start() error {
	e.stopCh = make(chan struct{})
	e.reqCh = make(chan request, 1024)
	e.state = stateRunning
	if e.role == RoleLeader {
		e.state |= stateLeaderBootStrapping
	}

	// start connect zk till connected
	e.connectZkTillSucceed(true)

	// start local user request server if need to
	if e.reqSrv.tcpHost != "" || e.reqSrv.unixHost != "" {
		if err := e.reqSrv.start(); err != nil {
			logrus.Warnf("[%s] Cannot start local request server: %v", e.Info().String(), err)
			return err
		}
	} else {
		logrus.Warnf("[%s] Request server disabled", e.Info().String())
	}

	for (e.state & stateRunning) != 0 {
		switch e.role {
		case RoleLeader:
			e.leaderLoop()
		case RoleFollower:
			e.followerLoop()
		case RoleCandidate:
			e.candidateLoop()
		}
	}

	logrus.Infof("[%d] stopped", e.id)
	return nil
}

// Stop the elector
func (e *ClusterElector) Stop() error {
	logrus.Infof("[%d] stopping", e.id)
	e.state = stateStopped
	close(e.stopCh)
	e.zkConnHandler.close(e)
	if e.reqSrv != nil {
		e.reqSrv.stop()
	}

	return nil
}

// Role return the role of the elector
func (e *ClusterElector) Role() Role {
	if (e.state & stateLeaderBootStrapping) != 0 {
		return RoleUnstable
	}
	return e.role
}

// Abdicate the leadership
func (e *ClusterElector) Abdicate() {
	e.reqCh <- reqAbdicate
}

// Promote as a leader
func (e *ClusterElector) Promote() {
	e.reqCh <- reqPromote
}

func (e *ClusterElector) doConnectZkSrv() error {
	c, _, err := zk.Connect(e.zkHost, 30*time.Second)
	if err != nil {
		logrus.Warnf("[%d] cannot connect zookeeper", e.id)
		return err
	}
	e.zkConn = c

	logrus.Infof("[%d] zk connection assigned", e.id)
	return nil
}

func (e *ClusterElector) doCloseZkSrv() error {
	if e.zkConn != nil {
		e.zkConn.Close()
	}
	return nil
}

func (e *ClusterElector) connectZkTillSucceed(start bool) {
	// NOTE: fd, 20180707
	// The third part zk client reconnect server internal, so
	// we have nothing to do with reconnection
	for e.state&stateRunning != 0 {
		if start || e.zkConn == nil {
			if err := e.zkConnHandler.connect(e); err != nil {
				logrus.Warnf("[%s] connect zk failed: %v", e.Info().String(), err)
				time.Sleep(time.Second)
			} else {
				logrus.Infof("[%d] will try my best to connect zk for the first time", e.id)
				return
			}
		} else {
			logrus.Infof("[%d] let zk client reconnect automatically", e.id)
			return
		}
	}
}

func (e *ClusterElector) changeRole(from, to Role) {
	if e.role == to || e.role != from {
		return
	}

	e.role = to

	logrus.Infof("[%s] role changed from %s to %s", e.Info().String(), from.String(), to.String())
	saveState(e.path, e.role, 0)
}

func (e *ClusterElector) leaderLoop() {
L:
	for (e.state&stateRunning) != 0 && e.role == RoleLeader {
		// still we need to try ascending myself
		// cuz we may just experience a short-time-crush-reboot, or
		// we may startup as a leader, etc...
		if err := e.ascend(); err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT

			case zk.ErrNodeExists:
				ok, err := e.zkLegalLeader()
				if err != nil {
					switch err {
					case zk.ErrNoServer:
						goto RECONNECT

					default:
						logrus.Infof("[%s] zkLegalLeader failed: %v", e.Info().String(), err)
						time.Sleep(time.Second)
						goto L
					}
				}

				if !ok {
					// the ascend has be done and role has been changed
					// clear the bit
					e.state &^= stateLeaderBootStrapping
					logrus.Infof("[%d] stateLeaderBootStrapping bit has been clear cuz role changed", e.id)
					e.changeRole(RoleLeader, RoleFollower)
					goto L
				}

			default:
				logrus.Infof("[%s] ascend failed: %v", e.Info().String(), err)
				time.Sleep(time.Second)
				goto L
			}
		}

		// the ascend has been done and we are still the leader
		// clear the bit
		e.state &^= stateLeaderBootStrapping
		logrus.Infof("[%d] stateLeaderBootStrapping bit has been clear cuz ascend succeed", e.id)

		// delete abdicate flag if any
		abdicating, err := e.isAbdicating()
		if err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT
			default:
				logrus.Infof("[%s] failed when checking if abdicating: %v", e.Info().String(), err)
				time.Sleep(time.Second)
				goto L
			}
		}
		if abdicating {
			if err := e.endAbdicate(); err != nil {
				switch err {
				case zk.ErrNoServer:
					goto RECONNECT
				default:
					logrus.Infof("[%s] failed when deleting abdicate flag: %v", e.Info().String(), err)
					time.Sleep(time.Second)
					goto L
				}
			}
		}

		// watch myself to check if anyone promote itself
		leaderWatchCh, err := e.watchLeader()
		if err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT
			default:
				logrus.Infof("[%s] failed when watch leader: %v", e.Info().String(), err)
				time.Sleep(time.Second)
				goto L
			}
		}

		select {
		case req := <-e.reqCh:
			// if user want to abdicate
			if req == reqAbdicate {
				logrus.Infof("[%d] user abdicating", e.id)

				// create abdicate flag
				if err := e.beginAbdicate(); err != nil {
					switch err {
					case zk.ErrNoServer:
						goto RECONNECT
					default:
						logrus.Infof("[%s] failed when creating abdicate flag node: %v", e.Info().String(), err)
						time.Sleep(time.Second)
						goto L
					}
				}

				// wait for others to promote
				// it is the new leader's duty to end abdication (delete abdicate flag)
				abdicateWatchCh, err := e.watchAbdicate()
				if err != nil {
					switch err {
					case zk.ErrNoServer:
						goto RECONNECT
					default:
						logrus.Infof("[%s] failed when watching abdicate: %v", e.Info().String(), err)
						time.Sleep(time.Second)
						goto L
					}
				}

				// do abdicate
				if err := e.zkAbdicate(); err != nil {
					switch err {
					case zk.ErrNoServer:
						goto RECONNECT
					default:
						logrus.Infof("[%s] failed when deleting leader node at zk: %v", e.Info().String(), err)
						time.Sleep(time.Second)
						goto L
					}
				}

				logrus.Infof("[%d] waiting for the followers to promote...", e.id)

				select {
				case ev := <-abdicateWatchCh:
					if ev.Type == zk.EventNodeDeleted {
						// abdicate finished
						logrus.Infof("[%d] abdicate finished", e.id)
						e.changeRole(RoleLeader, RoleFollower)
						goto L
					} else {
						logrus.Infof("[%s] abdicate not correct, get %v", e.Info().String(), ev)
						time.Sleep(time.Second)
						goto L
					}

				case <-e.stopCh:
					return
				}
			}

		case ev := <-leaderWatchCh:
			switch ev.Type {
			case zk.EventNodeDataChanged:
				// someone promote itself to a leader, follower him
				e.changeRole(RoleLeader, RoleFollower)
				goto L

			default:
				logrus.Warnf("[%s] something unexpected happened to the leader: %v", e.Info().String(), ev)
				time.Sleep(time.Second)
				goto L
			}

		case <-e.stopCh:
			return
		}
	}

RECONNECT:
	e.connectZkTillSucceed(false)
	goto L
}

func (e *ClusterElector) followerLoop() {
L:
	for (e.state&stateRunning) != 0 && e.role == RoleFollower {
		exist, leaderWatchCh, err := e.leaderExistsW()
		if err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT
			default:
				logrus.Infof("[%s] failed when checking and watching leader node: %v", e.Info().String(), err)
				time.Sleep(time.Second)
				goto L
			}
		}

		// if we already has a leader, wait for it to die
		// or appropriate if user ask
		if exist {
			select {
			case ev := <-leaderWatchCh:
				switch ev.Type {
				case zk.EventNodeDeleted:
					logrus.Infof("[%s] leader down", e.Info().String())
					goto TRY
				default:
					logrus.Warnf("[%s] something unexpected happened to the leader: %v", e.Info().String(), ev)
					goto L
				}

			case req := <-e.reqCh:
				if req == reqPromote {
					if e.appropriate() != nil {
						switch err {
						case zk.ErrNoServer:
							goto RECONNECT
						default:
							logrus.Infof("[%s] failed when appropriating: %v", e.Info().String(), err)
							time.Sleep(time.Second)
							goto L
						}
					}
					e.changeRole(RoleFollower, RoleLeader)
					goto L
				}

			case <-e.stopCh:
				return
			}
		}

		// if no leader for now, there are two cases:
		// 1. the leader is abdicating:
		// 		check if there is an abdicate flag: if yes, be a leader ASAP; if not, then goto 2.
		// 2. we saw the leader down, which means a watcher reports delete event on leader node:
		// 		set a timer for protection period for promotion, if the timer fired before leader raise up again,
		// 		be a leader; or stay in follower
	TRY:
		abdicating, err := e.isAbdicating()
		if err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT
			default:
				logrus.Infof("[%s] failed when checking abdicate flag: %v", e.Info().String(), err)
				time.Sleep(time.Second)
				goto L
			}
		}

		if abdicating {
			logrus.Infof("[%d] leader abdicating detected, will promote myself", e.id)
			if err := e.ascend(); err != nil {
				switch err {
				case zk.ErrNoServer:
					goto RECONNECT
				default:
					logrus.Infof("[%s] cannot ascend: %v", e.Info().String(), err)
					time.Sleep(time.Second)
					goto L
				}
			}
			e.changeRole(RoleFollower, RoleLeader)
			goto L
		} else {
			// the leader dead but not abdicated, try to go pass the protection period before claim the leadership,
			// but still watch if anyone promote before myself, or user ask me to be leader ASAP
			exist, leaderWatchCh, err = e.leaderExistsW()
			if err != nil {
				switch err {
				case zk.ErrNoServer:
					goto RECONNECT
				default:
					logrus.Infof("[%s] failed when checking and watching leader node: %v", e.Info().String(), err)
					time.Sleep(time.Second)
					goto L
				}
			}

			select {
			case <-time.After(time.Duration(e.options.protectionPeriod) * time.Second):
				if err := e.ascend(); err != nil {
					switch err {
					case zk.ErrNoServer:
						goto RECONNECT
					default:
						logrus.Infof("[%s] cannot ascend: %v", e.Info().String(), err)
						time.Sleep(time.Second)
						goto L
					}
				}
				e.changeRole(RoleFollower, RoleLeader)
				goto L

			case req := <-e.reqCh:
				if req == reqPromote {
					if err := e.ascend(); err != nil {
						switch err {
						case zk.ErrNoServer:
							goto RECONNECT
						default:
							logrus.Infof("[%s] failed when being a leader: %v", e.Info().String(), err)
							time.Sleep(time.Second)
							goto L
						}
					}
					e.changeRole(RoleFollower, RoleLeader)
					goto L
				}

			case thing := <-leaderWatchCh:
				// no matter what happened to the leader, we ignore and stay in follower
				logrus.Infof("[%s] something happened to the leader: %v", e.Info().String(), thing)
				goto L
			}
		}
	}

RECONNECT:
	e.connectZkTillSucceed(false)
	goto L
}

func (e *ClusterElector) candidateLoop() {
L:
	for (e.state&stateRunning) != 0 && e.role == RoleCandidate {
		err := e.ascend()
		if err != nil {
			switch err {
			case zk.ErrNoServer:
				goto RECONNECT
			case zk.ErrNodeExists:
				logrus.Infof("[%s] already has a leader at zk: %s", e.Info().String(), e.zkLeaderPath)
				e.changeRole(RoleCandidate, RoleFollower)
				goto L
			default:
				// zk errors, retry as a candidate
				logrus.Infof("[%s] shit happened when trying ascend: %v", e.Info().String(), err)
				time.Sleep(time.Second)
			}
		}
		e.changeRole(RoleCandidate, RoleLeader)
		goto L
	}

RECONNECT:
	e.connectZkTillSucceed(false)
	goto L
}

func (e *ClusterElector) isAbdicating() (bool, error) {
	exist, _, err := e.zkConn.Exists(e.zkAbdicatePath)
	return exist, err
}

func (e *ClusterElector) beginAbdicate() error {
	_, err := e.zkConn.Create(e.zkAbdicatePath, []byte(strconv.FormatUint(e.id, 10)), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (e *ClusterElector) watchAbdicate() (<-chan zk.Event, error) {
	_, _, ch, err := e.zkConn.GetW(e.zkAbdicatePath)
	return ch, err
}

func (e *ClusterElector) endAbdicate() error {
	return e.zkConn.Delete(e.zkAbdicatePath, -1)
}

// [ZK ops]
// claim the leadership event if there already has a leader
func (e *ClusterElector) appropriate() error {
	exist, _, err := e.zkConn.Exists(e.zkLeaderPath)

	if err != nil {
		return err
	}

	if exist {
		_, err := e.zkConn.Set(e.zkLeaderPath, []byte(strconv.FormatUint(e.id, 10)), -1)
		return err
	}

	return e.ascend()
}

func (e *ClusterElector) zkAbdicate() error {
	return e.zkConn.Delete(e.zkLeaderPath, -1)
}

func (e *ClusterElector) zkLegalLeader() (bool, error) {
	b, _, err := e.zkConn.Get(e.zkLeaderPath)
	if err != nil {
		return false, err
	}

	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return false, err
	}

	if id == e.id {
		return true, nil
	}

	return false, nil
}

func (e *ClusterElector) leaderExistsW() (bool, <-chan zk.Event, error) {
	exists, _, ch, err := e.zkConn.ExistsW(e.zkLeaderPath)
	if err != nil {
		return false, nil, err
	}
	if exists {
		return true, ch, nil
	}
	return false, nil, nil
}

// [ZK ops]
// try to be a leader
func (e *ClusterElector) ascend() error {
	_, err := e.zkConn.Create(e.zkLeaderPath, []byte(strconv.FormatUint(e.id, 10)), zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll))
	return err
}

func (e *ClusterElector) watchLeader() (<-chan zk.Event, error) {
	_, _, ch, err := e.zkConn.GetW(e.zkLeaderPath)
	return ch, err
}
