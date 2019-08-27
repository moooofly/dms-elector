package server

import (
	"errors"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/moooofly/dms-elector/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

/********** Master-slave mode elector **********/
// elector (internal) event
type eEvent struct {
	event interface{} // event into elector
	reply interface{} // event out, maybe nil
	errCh chan error  // err chan, used as event handled signal as well
}

// MSElector is the elector used in master-slave mode
type MSElector struct {
	id    uint64 // elector id
	role  Role   // elector role
	epoch uint64 // elector current epoch
	count uint64 // ping counter as a leader
	bid   uint64 // bid as a candidate

	state electorState // elector running state

	path string // persistence file path

	localEleHost  string // local election server host
	remoteEleHost string // remote election server host

	reqSrv *requestServer // user request server

	eleSrvLnr net.Listener // local election server listener
	eleSrv    *grpc.Server // local election server

	eleConnHandler *connHandler     // connection handler of elector
	eleCliConn     *grpc.ClientConn // connection with remote election server
	eleCli         pb.ElectorClient // client to remote election server

	stopCh chan struct{} // stop signal
	evCh   chan *eEvent  // elector event channel

	timer *time.Timer // general used timer

	options electorOptions // options
}

func doConnectRemoteEleSrvWrapper(args ...interface{}) error {
	e := args[0].(*MSElector)
	return e.doConnectRemoteEleSrv()
}

func doCloseRemoteEleSrvWrapper(args ...interface{}) error {
	e := args[0].(*MSElector)
	return e.doCloseRemoteEleSrv()
}

// NewMSElector is the constructor of MSElector
func NewMasterSlave(
	stateFile string,
	rsTcpHost, rsUnixHost string,
	local, remote string,
	opts ...ElectorOption,
) *MSElector {

	role, epoch := loadState(stateFile)
	return newMasterSlaveWithInfo(stateFile, role, epoch, rsTcpHost, rsUnixHost, local, remote, opts...)
}

func newMasterSlaveWithInfo(path string, role Role, epoch uint64, reqSrvHost, reqSrvPath, localEleHost, remoteEleHost string, opts ...ElectorOption) *MSElector {
	rand.Seed(time.Now().UnixNano())
	ele := new(MSElector)

	ele.id = rand.Uint64()
	ele.role = role
	ele.epoch = epoch
	ele.count = 0
	ele.bid = rand.Uint64()

	ele.state = stateStopped
	ele.path = path

	ele.reqSrv = newRequestServer(reqSrvHost, reqSrvPath, ele)

	ele.localEleHost = localEleHost
	ele.remoteEleHost = remoteEleHost

	// some default values
	ele.options.eleConnTimeout = 30
	ele.options.seekVotePeriod = 1
	ele.options.seekVoteMaxTry = 5
	ele.options.leaderTimeout = 15
	ele.options.pingPeriod = 1
	ele.options.leaderBootstrapPeriod = 0

	// apply options if any
	for _, o := range opts {
		o(&ele.options)
	}

	ele.eleConnHandler = &connHandler{connState: connStateDisconnect, connectF: nil, closeF: nil}
	ele.eleConnHandler.registerConnFunc(doConnectRemoteEleSrvWrapper)
	ele.eleConnHandler.registerCloseFunc(doCloseRemoteEleSrvWrapper)

	return ele
}

// Info the elector
func (e *MSElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start the elector
func (e *MSElector) Start() error {
	if (e.state & stateRunning) != 0 {
		return errors.New("has been started already")
	}

	e.state = stateRunning
	if e.role == RoleLeader && e.options.leaderBootstrapPeriod != 0 {
		e.state |= stateLeaderBootStrapping
	}

	e.stopCh = make(chan struct{})
	e.evCh = make(chan *eEvent, 1024)

	// start local election server, return if failed
	err := e.startLocalEleSrv()
	if err != nil {
		logrus.Warnf("[%s] Cannot start local election server: %v", e.Info().String(), err)
		return err
	}
	logrus.Infof("[%s] Local election server started", e.Info().String())

	// start local user request server
	if e.reqSrv.tcpHost != "" || e.reqSrv.unixHost != "" {
		if err := e.reqSrv.start(); err != nil {
			logrus.Warnf("[%s] Cannot start local request server: %v", e.Info().String(), err)
			return err
		}
	} else {
		logrus.Warnf("[%s] Request server disabled", e.Info().String())
	}

	// connect to the other side elector
	go func() {
		for {
			err = e.eleConnHandler.connect(e)
			if err != nil {
				logrus.Errorf("[%s] cannot connect to the other-side-elector when starting, wait for another try...",
					e.Info().String())
			} else {
				return
			}
		}
	}()

	// main election loop
	for (e.state & stateRunning) != 0 {
		logrus.Infof("[%s] running as a %s now, at epoch %d", e.Info().String(), e.role.String(), e.epoch)

		switch e.role {
		case RoleCandidate:
			e.candidateLoop()
		case RoleFollower:
			e.followerLoop()
		case RoleLeader:
			e.leaderLoop()
		default:
			logrus.Warnf("[%s] not an illegal role: %v, quitting", e.Info().String(), e.role)
			return errors.New("not an illegal role")
		}
	}

	return nil
}

// Stop the elector
func (e *MSElector) Stop() {
	if (e.state & stateRunning) == 0 {
		return
	}

	logrus.Infof("[%s] stopping the elector", e.Info().String())

	e.state = stateStopped
	close(e.stopCh)
	if e.eleSrvLnr != nil {
		e.eleSrvLnr.Close()
	}
	if e.eleSrv != nil {
		e.eleSrv.Stop()
	}
	e.eleConnHandler.close(e)
	if e.reqSrv != nil {
		e.reqSrv.stop()
	}
	saveState(e.path, e.role, e.epoch)
}

// Role return the role of the elector
func (e *MSElector) Role() Role {
	if (e.state & stateLeaderBootStrapping) != 0 {
		return RoleUnstable
	}
	return e.role
}

// Abdicate the leadership
func (e *MSElector) Abdicate() {
	if (e.state&stateRunning) == 0 || e.role != RoleLeader {
		return
	}

	e.abdicate()
}

// Promote as a leader
func (e *MSElector) Promote() {
	if (e.state&stateRunning) == 0 || e.role == RoleLeader {
		return
	}

	e.promote()
}

// Connect to the remote elector server
func (e *MSElector) connectRemoteEleSrv() (*grpc.ClientConn, error) {
	logrus.Infof("[%s] will try to connect remote election server", e.Info().String())

	conn, err := grpc.Dial(
		e.remoteEleHost,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(1*time.Second),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 30 * time.Second}))

	if err != nil {
		logrus.Warnf("[%s] cannot connect to remote election server: %v", e.Info().String(), err)
		return nil, err
	}

	logrus.Infof("[%s] connected", e.Info().String())
	return conn, nil
}

// Do the real shit to connect with the remote election server
// Should be registered as the connect function of connection by a layer of wrapper
func (e *MSElector) doConnectRemoteEleSrv() error {
	// NOTE: 20181011, fd
	// introduce a new algorithm to try to connect remote elector server every pingPeriod second util
	// connected, or timeout
	connC := make(chan *grpc.ClientConn)
	ticker := time.NewTicker(time.Duration(e.options.pingPeriod) * time.Second)
	once := new(sync.Once)

	for {
		select {
		case <-time.After(time.Duration(e.options.eleConnTimeout) * time.Second):
			logrus.Warnf("[%s] connection timeout!", e.Info().String())
			return errors.New("connect failed")

		case c := <-connC:
			e.eleCliConn = c
			e.eleCli = pb.NewElectorClient(c)
			return nil

		case <-ticker.C:
			go func() {
				conn, err := e.connectRemoteEleSrv()
				if err == nil {
					once.Do(func() {
						ticker.Stop()
						connC <- conn
					})
				}
			}()
		}
	}

	// should not be here
	return errors.New("connect failed")
}

// Do the real shit to disconnect with the remote election server
// Should be registered as the close function of connection by a layer of wrapper
func (e *MSElector) doCloseRemoteEleSrv() error {
	if e.eleCliConn != nil {
		return e.eleCliConn.Close()
	}
	return nil
}

// Start the local election server
func (e *MSElector) startLocalEleSrv() error {
	l, err := net.Listen("tcp", e.localEleHost)
	if err != nil {
		logrus.Warnf("[%s] cannot start local election server", e.Info().String())
		return err
	}

	srv := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second,
		Timeout: 30 * time.Second}))
	pb.RegisterElectorServer(srv, &eleSrv{e})

	e.eleSrvLnr = l
	e.eleSrv = srv

	go e.eleSrv.Serve(e.eleSrvLnr)

	return nil
}

// Change elector's role from one to another, at a specific epoch
func (e *MSElector) changeRole(from, to Role, epoch uint64) {
	if e.role == to || e.role != from {
		return
	}

	e.role = to
	e.epoch = epoch
	e.count = 0

	saveState(e.path, e.role, e.epoch)
	logrus.Infof("[%s] role changed from %s to %s at epoch %d", e.Info().String(), from.String(), to.String(), epoch)
}

func (e *MSElector) nextEpoch() uint64 {
	return e.epoch + 1
}

// Ping the other elector
func (e *MSElector) ping() {
	if (e.state&stateRunning) == 0 || e.role != RoleLeader || e.eleCli == nil || e.eleConnHandler.connState == connStateConnecting {
		logrus.Debugf("[%s] refuse ping cause: s.state=%x, e.role=%v, e.eleCli=%v, e.connState=%s",
			e.Info().String(), e.state, e.role, e.eleCli, e.eleConnHandler.state().String())
		return
	}

	var ping pb.MsgPING

	ping.Id, ping.Role, ping.Epoch, ping.Count = e.id, pb.EnumRole(e.role), e.epoch, e.count

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.eleConnTimeout))
	defer cancel()
	r, err := e.eleCli.PING(ctx, &ping)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.eleConnHandler.connect(e); err != nil {
				logrus.Infof("[%s] reconnect after a failed-ping failed: %v", e.Info().String(), err)
			}
		default:
		}
		logrus.Infof("[%s] ping failed: %v", e.Info().String(), err)
		return
	}
	e.evCh <- &eEvent{r, nil, nil}
}

// Seek vote from the other elector
func (e *MSElector) seekVote() {
	if (e.state&stateRunning) == 0 || e.role != RoleCandidate || e.eleCli == nil || e.eleConnHandler.connState == connStateConnecting {
		logrus.Debugf("[%s] refuse seekVote cause: s.state=%x, e.role=%v, e.eleCli=%v, e.connState=%s",
			e.Info().String(), e.state, e.role, e.eleCli, e.eleConnHandler.connState.String())
		return
	}

	var seekVote pb.MsgSeekVote
	seekVote.Id, seekVote.Role, seekVote.Epoch, seekVote.Bid = e.id, pb.EnumRole(e.role), e.epoch, e.bid

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.eleConnTimeout))
	defer cancel()
	vote, err := e.eleCli.SeekVote(ctx, &seekVote)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.eleConnHandler.connect(e); err != nil {
				logrus.Infof("[%s] reconnect after a failed-seekvote failed: %v", e.Info().String(), err)
			}
		default:
		}
		logrus.Infof("[%s] seekVote failed: %v", e.Info().String(), err)
		return
	}
	e.evCh <- &eEvent{vote, nil, nil}
}

// Abdicate the leadership to the other elector
// NOTE: fd, 20180522
// the strategy for abdication in elector version alpha.3 updates to:
// 1. set the abdicating bit;
// 2. put an abdicate-user-request-event into elector machine loop to turn to follower **IMMEDIATELY**;
// 3. send an abdicate-message to the other side to make him turn to a leader;
// 4. wait for the reply, which should be a promoted-message, put it to elector machine loop to clear the
// 	  abdicating bit.
func (e *MSElector) abdicate() {
	if (e.state&stateRunning) == 0 || (e.state&stateRoleChanging) != 0 || e.role != RoleLeader || e.eleCli == nil {
		logrus.Debugf("[%s] refuse abdicate user request cause: s.state=%x, e.role=%v, e.eleCli=%v",
			e.Info().String(), e.state, e.role, e.eleCli)
		return
	}

	// NOTE: fd, 20180629
	// we should make local elector abdicated even if connection with other side is lost

	var abdicate pb.MsgAbdicate

	e.state |= stateAbdicating
	abdicate.Id, abdicate.Role, abdicate.Epoch = e.id, pb.EnumRole(e.role), e.epoch
	e.evCh <- &eEvent{userRequestAbdicate, nil, nil}

	logrus.Infof("[%s] signal of abdicate on the local side has been send", e.Info().String())

	if e.eleConnHandler.connState == connStateConnecting {
		logrus.Debugf("[%s] skip sending abdicate user request cause connecting to the other side elector",
			e.Info().String())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.eleConnTimeout))
	defer cancel()
	promoted, err := e.eleCli.Abdicate(ctx, &abdicate)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.eleConnHandler.connect(e); err != nil {
				logrus.Infof("[%s] reconnect after a failed-abdicate failed: %v", e.Info().String(), err)
			}
		default:
		}
		logrus.Infof("[%s] abdicate failed: %v", e.Info().String(), err)
		return
	}
	e.evCh <- &eEvent{promoted, nil, nil}
}

// Promote myself
// NOTE: fd, 20180522
// the strategy for promotion in elector version alpha.3 updates to:
// 1. set the promoting bit;
// 2. put an promote-user-request-event into elector machine loop to turn to leader **IMMEDIATELY**,
//    the promoting bit will be clear after the role has been changed.
func (e *MSElector) promote() {
	if (e.state&stateRunning) == 0 || (e.state&stateRoleChanging) != 0 || e.role == RoleLeader {
		logrus.Debugf("[%s] refuse promote user request cause: s.state=%x, e.role=%v, e.eleCli=%v",
			e.Info().String(), e.state, e.role, e.eleCli)
		return
	}

	logrus.Infof("[%s] handle promote user request", e.Info().String())

	e.state |= statePromoting
	e.evCh <- &eEvent{userRequestPromote, nil, nil}
}

// Leader main loop:
// 1. ping the other side;
// 2. handle user request: abdicate;
// 3. handle received message:
//		1) PING: brain-split detected, change to follower if:
//					a. my epoch is smaller;
//					b. the epochs are equal, but my ping counter is smaller.
//		2) PONG: change to follower if the [role:epoch:count] in PONG message says the other side is THE right leader;
//		3) SeekVote: reject;
//		4) Vote: nothing to do, ignore;
// 		5) Abdicate: nothing to do in fact, but reply anyway;
//		6) Promoted: nothing to do.
func (e *MSElector) leaderLoop() {
	var pingTicker = time.NewTicker(time.Duration(e.options.pingPeriod) * time.Second)
	var once = new(sync.Once)

	for (e.state&stateRunning) != 0 && e.role == RoleLeader {
		// leader bootstrap timeout handler
		if (e.state & stateLeaderBootStrapping) != 0 {
			once.Do(func() {
				go time.AfterFunc(time.Duration(e.options.leaderBootstrapPeriod)*time.Second, func() {
					logrus.Debugf("[%s] clear stateLeaderBootStrapping bit cuz timeout", e.Info().String())
					e.state &^= stateLeaderBootStrapping
				})
				logrus.Debugf("[%s] leaderBootstrapTimer has been set", e.Info().String())
			})
		}

		select {
		case <-pingTicker.C:
			// increase ping counter at each tick
			e.count++
			go e.ping()

		case eev := <-e.evCh:
			ev := eev.event
			logrus.Debugf("[%s] get event: %#v", e.Info().String(), ev)

			switch ev.(type) {
			// user requests, only handler abdicate request
			case userRequest:
				if req := ev.(userRequest); req == userRequestAbdicate {
					e.changeRole(RoleLeader, RoleFollower, e.epoch)
				}

			case *pb.MsgPING:
				pev := ev.(*pb.MsgPING)
				// brain-split recovery
				if e.epoch < pev.Epoch || (e.epoch == pev.Epoch && e.count < pev.Count) {
					logrus.Infof("[%s] will change to follower because an elder leader", e.Info().String())
					e.changeRole(RoleLeader, RoleFollower, pev.Epoch)
				}
				eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: e.count}
				eev.errCh <- nil

			case *pb.MsgPONG:
				pev := ev.(*pb.MsgPONG)
				// check if the other side is a good leader
				if e.epoch < pev.Epoch || (e.epoch == pev.Epoch && e.count < pev.Count) {
					logrus.Infof("[%s] will change to follower because i am not old enough", e.Info().String())
					e.changeRole(RoleLeader, RoleFollower, pev.Epoch)
				}

			case *pb.MsgSeekVote:
				// refuse
				eev.reply = pb.MsgVote{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Agreed: false}
				eev.errCh <- nil

			case *pb.MsgVote:
				// nothing to do

			case *pb.MsgAbdicate:
				eev.reply = pb.MsgPromoted{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Promoted: false}
				eev.errCh <- nil

			case *pb.MsgPromoted:
				// nothing to do

			default:
				logrus.Warnf("[%s] not a good event: %T", e.Info().String(), ev)
			}

			// 20180425, fd, #leaderBootstrapPeriod
			// the first communication has been finished, the elector MUST know
			// whether he is the right leader now, so clear the stateLeaderBootStrapping bit
			if (e.state & stateLeaderBootStrapping) != 0 {
				logrus.Debugf("[%s] clear stateLeaderBootStrapping bit cuz first communication finished", e.Info().String())
				e.state &^= stateLeaderBootStrapping
			}

		case <-e.stopCh:
			break
		}
	}
}

// Follower main loop:
// 1. handle user request: promote;
// 2. handle received message:
//		1) PING:
//				a. reset the leader timeout timer;
//				b. update the epoch if needed;
//				c. reply a PONG.
//		2) PONG: nothing to do, ignore;
//		3) SeekVote: reject;
//		4) Vote: nothing to do, ignore;
// 		5) Abdicate: promote and reply a promoted message, if I am not already in abdicating state;
//		6) Promoted: clear the abdicating flag if any.
func (e *MSElector) followerLoop() {
	e.timer = time.NewTimer(time.Duration(e.options.leaderTimeout) * time.Second)

	for (e.state&stateRunning) != 0 && e.role == RoleFollower {
		select {
		case <-e.timer.C:
			// leader timeout
			logrus.Infof("[%s] leader timeout!", e.Info().String())
			e.timer = nil
			// NOTE: fd, 20180522
			// if the follower is in abdicating process and the other side crush/connection-lost,
			// we may never receive the Promoted message which used to clear the abdicating bit,
			// clear it before turn to leader
			e.state &^= stateAbdicating
			e.changeRole(RoleFollower, RoleLeader, e.nextEpoch())
			break

		case eev := <-e.evCh:
			ev := eev.event
			logrus.Debugf("[%s] get event: %#v", e.Info().String(), ev)

			switch ev.(type) {
			// user requests, only handler promote request
			case userRequest:
				if req := ev.(userRequest); req == userRequestPromote {
					e.changeRole(e.role, RoleLeader, e.nextEpoch())
					e.state &^= statePromoting
				}

			case *pb.MsgPING:
				pev := ev.(*pb.MsgPING)
				if e.epoch != pev.Epoch {
					e.epoch = pev.Epoch
					saveState(e.path, e.role, e.epoch)
				}

				if !e.timer.Stop() {
					<-e.timer.C
				}
				e.timer.Reset(time.Duration(e.options.leaderTimeout) * time.Second)

				// NOTE: fd, 20181214
				// let follower reply a pong with the same count as the ping just received
				eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: pev.Count}
				eev.errCh <- nil

			case *pb.MsgPONG:
				// nothing to do

			case *pb.MsgSeekVote:
				// refuse
				eev.reply = pb.MsgVote{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Agreed: false}
				eev.errCh <- nil

			case *pb.MsgVote:
				// nothing to do

			case *pb.MsgAbdicate:
				// NOTE: fd, 20180522
				// if the elector is already in abdicating process, which means the other side will
				// turn to a leader, but still accept abdicate request send from the other side, which
				// will turn myself to a leader, there has a chance to get a L-L-brain-split state,
				// so deny it
				if (e.state & stateAbdicating) == 0 {
					logrus.Infof("[%s] get an abdicate, will promote", e.Info().String())
					e.changeRole(RoleFollower, RoleLeader, e.nextEpoch())
					eev.reply = pb.MsgPromoted{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Promoted: true}
					eev.errCh <- nil
				} else {
					eev.errCh <- errors.New("already in abdicating state")
				}

			case *pb.MsgPromoted:
				logrus.Infof("[%s] get a promoted, will clear abdicating bit", e.Info().String())
				e.state &^= stateAbdicating

			default:
				logrus.Debugf("not a good event: %T", ev)
			}

		case <-e.stopCh:
			break
		}
	}
}

// Candidate main loop:
// 1. seek vote from the other side;
// 2. handle received message:
//		1) PING: be a follower;
//		2) PONG: nothing to do, ignore;
//		3) SeekVote: agree if my bid is smaller;
//		4) Vote: promote if the other elector agreed;
// 		5) Abdicate: promote, reply a promoted message;
//		6) Promoted: nothing to do, ignore.
func (e *MSElector) candidateLoop() {
	var cnt uint
	var seekVoteTicker = time.NewTicker(time.Second * time.Duration(e.options.seekVotePeriod))

	for (e.state&stateRunning) != 0 && e.role == RoleCandidate {
		select {
		case <-seekVoteTicker.C:
			cnt++
			if cnt > e.options.seekVoteMaxTry {
				e.changeRole(RoleCandidate, RoleLeader, e.nextEpoch())
				break
			}
			go e.seekVote()

		case eev := <-e.evCh:
			ev := eev.event
			logrus.Debugf("[%s] get event: %#v", e.Info().String(), ev)

			switch ev.(type) {
			case *pb.MsgPING:
				pev := ev.(*pb.MsgPING)
				e.changeRole(RoleCandidate, RoleFollower, pev.Epoch)
				eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: e.count}
				eev.errCh <- nil

			case *pb.MsgPONG:
				// nothing to do

			case *pb.MsgSeekVote:
				var agreed bool
				svev := ev.(*pb.MsgSeekVote)
				if svev.Bid > e.bid {
					agreed = true
				}
				eev.reply = pb.MsgVote{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Agreed: agreed}
				eev.errCh <- nil

			case *pb.MsgVote:
				vev := ev.(*pb.MsgVote)
				if vev.Agreed {
					e.changeRole(RoleCandidate, RoleLeader, e.nextEpoch())
				} else {
					if vev.Role == pb.EnumRole_LEADER {
						e.changeRole(RoleCandidate, RoleFollower, vev.Epoch)
					}
				}

			case *pb.MsgAbdicate:
				e.changeRole(RoleCandidate, RoleLeader, e.nextEpoch())
				eev.reply = pb.MsgPromoted{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Promoted: true}
				eev.errCh <- nil
				logrus.Infof("[%s] get an abdicate, will promote", e.Info().String())

			case *pb.MsgPromoted:
				// nothing to do

			default:
				logrus.Debugf("not a good event: %T", ev)
			}

		case <-e.stopCh:
			break
		}
	}
}

/********** master-slave mode election gRPC server **********/
type eleSrv struct {
	e *MSElector
}

func (es *eleSrv) sanityCheck(ctx context.Context) error {
	var pr *peer.Peer
	var ok bool
	var from, expect string

	pr, ok = peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.DataLoss, "failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		return status.Error(codes.DataLoss, "failed to get peer address")
	}

	from, expect = strings.Split(pr.Addr.String(), ":")[0], strings.Split(es.e.remoteEleHost, ":")[0]
	if from != expect {
		logrus.Infof("[%s] has refused a PING from [%s] because unexpected ip", es.e.Role().String(), from)
		return errors.New("not the target host to connect")
	}

	return nil
}

func (es *eleSrv) PING(ctx context.Context, ping *pb.MsgPING) (*pb.MsgPONG, error) {
	var eev eEvent

	if err := es.sanityCheck(ctx); err != nil {
		return nil, err
	}

	eev.event = ping
	eev.errCh = make(chan error)

	es.e.evCh <- &eev

	if err := <-eev.errCh; err != nil {
		s, _ := status.FromError(err)
		return nil, s.Err()
	}

	reply := eev.reply.(pb.MsgPONG)
	return &reply, nil
}

func (es *eleSrv) SeekVote(ctx context.Context, seek *pb.MsgSeekVote) (*pb.MsgVote, error) {
	var eev eEvent

	if err := es.sanityCheck(ctx); err != nil {
		return nil, err
	}

	eev.event = seek
	eev.errCh = make(chan error)

	es.e.evCh <- &eev

	if err := <-eev.errCh; err != nil {
		s, _ := status.FromError(err)
		return nil, s.Err()
	}

	reply := eev.reply.(pb.MsgVote)
	return &reply, nil
}

func (es *eleSrv) Abdicate(ctx context.Context, abdicate *pb.MsgAbdicate) (*pb.MsgPromoted, error) {
	var eev eEvent

	if err := es.sanityCheck(ctx); err != nil {
		return nil, err
	}

	eev.event = abdicate
	eev.errCh = make(chan error)

	es.e.evCh <- &eev

	if err := <-eev.errCh; err != nil {
		s, _ := status.FromError(err)
		return nil, s.Err()
	}

	reply := eev.reply.(pb.MsgPromoted)
	return &reply, nil
}
