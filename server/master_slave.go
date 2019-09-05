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

type electorOptions struct {
	/************************* MS mode options *************************/
	connTimeout            uint // connection timeout
	seekVotePeriod         uint // seek vote period
	seekVoteMaxTry         uint // seek vote max try
	pingPeriod             uint // ping period
	leaderTimeoutThreshold uint // leader timeout

	// 20180425, fd, #leaderBootstrapPeriod
	// The idea of `Leader Bootstrap Period' is introduced for slow-leader-bootstrap.
	// When an elector startup as a leader, there might already be a leader alive, which will led to brain-split,
	// even though this situation will be solved sooner or later, we do like to sweep this
	// useless and error-prone period, especially as a DMS-Detector, which might make HaProxy redirect user
	// connections to an unstable backend address when more than one leader alive at the same time.
	// After the introduction of `Leader Bootstrap Period', a startup-leader will mark himself
	// in leaderBootstrapState state, until:
	// 1. for ms mode:
	// 	  1.1 the every first communication succeed with the other elector (ms mode), or
	//    1.2 timeout for leaderBootstrapState state (leaderBootstrapPeriod)
	// 2. for cluster mode:
	//    the every first try of ascend (cluster mode),
	// to get the right role (follower instead, or leader still);
	// during this state, all user requests for elector's role will be replied as RoleUnstable.
	leaderBootstrapPeriod uint

	/************************* CLUSTER mode options *************************/
	protectionPeriod uint // protection period for leader
}

// electorOption config how the elector works
type electorOption func(o *electorOptions)

// WithLeaderBootStrapPeriod set the period among sending ping
func WithLeaderBootStrapPeriod(period uint) electorOption {
	return func(o *electorOptions) {
		o.leaderBootstrapPeriod = period
	}
}

// WithEleConnTimeout set the timeout for connect remote elector server
func WithEleConnTimeout(timeout uint) electorOption {
	return func(o *electorOptions) {
		o.connTimeout = timeout
	}
}

// WithSeekVotePeriod set the period among seeking vote
func WithSeekVotePeriod(period uint) electorOption {
	return func(o *electorOptions) {
		o.seekVotePeriod = period
	}
}

// WithSeekVoteMaxTry set the max time for seeking vote
func WithSeekVoteMaxTry(try uint) electorOption {
	return func(o *electorOptions) {
		o.seekVoteMaxTry = try
	}
}

// WithLeaderTimeout set how long we thought the leader is unreachable
func WithLeaderTimeout(timeout uint) electorOption {
	return func(o *electorOptions) {
		o.leaderTimeoutThreshold = timeout
	}
}

// WithPingPeriod set the period among sending ping
func WithPingPeriod(period uint) electorOption {
	return func(o *electorOptions) {
		o.pingPeriod = period
	}
}

// WithProtectionPeriod set the period of leader protection
func WithProtectionPeriod(period uint) electorOption {
	return func(o *electorOptions) {
		o.protectionPeriod = period
	}
}

/********** Master-slave mode elector **********/
// elector (internal) event
type eEvent struct {
	event interface{} // event into elector
	reply interface{} // event out, maybe nil
	errCh chan error  // err chan, used as event handled signal as well
}

// msElector is the elector used in master-slave mode
type msElector struct {
	id uint64 // elector id

	role   Role   // elector role
	epoch  uint64 // elector current epoch
	stFile string // state file with role and epoch

	count uint64 // ping counter as a leader
	bid   uint64 // bid as a candidate

	state electorState // elector running state

	local  string // local elector listening address
	remote string // remote elector listening address

	rs *roleService // user request server

	eleSrvLnr net.Listener // local election server listener
	eleSrv    *grpc.Server // local election server

	handler *connHandler // connection handler of elector

	clientConn *grpc.ClientConn // client connection to remote elector
	eleCli     pb.ElectorClient // grpc client to remote elector

	stopCh chan struct{}
	evCh   chan *eEvent

	timer *time.Timer

	options electorOptions
}

// FIXME: only args[0] be used
func doRemoteConnectWrapper(args ...interface{}) error {
	e := args[0].(*msElector)
	return e.doRemoteConnect()
}

func doRemoteDisconnectWrapper(args ...interface{}) error {
	e := args[0].(*msElector)
	return e.doRemoteDisconnect()
}

// NewmsElector is the constructor of msElector
func NewMasterSlave(
	stfile string,
	rsTcpHost, rsUnixHost string,
	local, remote string,
	opts ...electorOption,
) *msElector {

	role, epoch := loadState(stfile)
	return newMasterSlaveWithInfo(stfile, role, epoch, rsTcpHost, rsUnixHost, local, remote, opts...)
}

func newMasterSlaveWithInfo(
	stfile string,
	role Role,
	epoch uint64,
	rsTcpHost, rsUnixHost string,
	local, remote string,
	opts ...electorOption,
) *msElector {
	ele := new(msElector)

	ele.id = rand.Uint64()
	ele.bid = rand.Uint64()

	ele.role = role
	ele.epoch = epoch
	ele.count = 0

	ele.state = stateStopped
	ele.stFile = stfile

	ele.rs = newRoleService(rsTcpHost, rsUnixHost, ele)

	ele.local = local
	ele.remote = remote

	// TODO
	// some default values
	ele.options.connTimeout = 30
	ele.options.seekVotePeriod = 1
	ele.options.seekVoteMaxTry = 5
	ele.options.leaderTimeoutThreshold = 15
	ele.options.pingPeriod = 1
	ele.options.leaderBootstrapPeriod = 0

	// apply options if any
	for _, o := range opts {
		o(&ele.options)
	}

	ele.handler = &connHandler{connState: connStateDisconnect, connectF: nil, closeF: nil}
	ele.handler.registerConnFunc(doRemoteConnectWrapper)
	ele.handler.registerCloseFunc(doRemoteDisconnectWrapper)

	return ele
}

// Info gets metadata of the elector
func (e *msElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start launch a master-slave elector
func (e *msElector) Start() error {
	if (e.state & stateRunning) != 0 {
		return errors.New("has been started already")
	}

	e.state = stateRunning
	if e.role == RoleLeader && e.options.leaderBootstrapPeriod != 0 {
		e.state |= stateLeaderBootStrapping
	}

	e.stopCh = make(chan struct{})
	// FIXME: why
	e.evCh = make(chan *eEvent, 1024)

	// 启动 elector server
	err := e.launchElector()
	if err != nil {
		logrus.Warnf("[master-slave] [%s] launch elector at [%s] failed: %v", e.Info().String(), e.local, err)
		return err
	}
	logrus.Infof("[master-slave] [%s] launch elector at [%s] success", e.Info().String(), e.local)

	// FIXME: 是否应该不断一直重连
	go func() {
		for {
			// connect to remote elector
			err = e.handler.connect(e)
			if err != nil {
				logrus.Warningf("[master-slave] connect to remote[%s] failed, reason: %v", e.remote, err)
				return
			} else {
				logrus.Infof("[master-slave] connect to remote[%s] success", e.remote)
				return
			}
		}
	}()

	// main election loop
	//for (e.state & stateRunning) != 0 {
	if (e.state & stateRunning) != 0 {
		logrus.Infof("[master-slave] [%s] running as [%s] at epoch [%d]", e.Info().String(), e.role.String(), e.epoch)

		switch e.role {
		case RoleCandidate:
			go e.candidateLoop()
		case RoleFollower:
			go e.followerLoop()
		case RoleLeader:
			go e.leaderLoop()
		default:
			logrus.Warnf("[master-slave] [%s] is not a legal role", e.role.String())
			return errors.New("not a legal role")
		}
	}

	// 启动 role service
	if err := e.rs.Start(); err != nil {
		logrus.Warnf("[master-slave] start grpc-role-service failed, reason: %v", err)
		return err
	}

	return nil
}

// Stop the elector
func (e *msElector) Stop() {
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
	e.handler.close(e)
	if e.rs != nil {
		e.rs.Stop()
	}
	saveState(e.stFile, e.role, e.epoch)
}

// Role gets the role of the elector
func (e *msElector) Role() Role {
	if (e.state & stateLeaderBootStrapping) != 0 {
		return RoleUnstable
	}
	return e.role
}

// Abdicate the leadership
func (e *msElector) Abdicate() {
	if (e.state&stateRunning) == 0 || e.role != RoleLeader {
		return
	}

	e.abdicate()
}

// Promote as a leader
func (e *msElector) Promote() {
	if (e.state&stateRunning) == 0 || e.role == RoleLeader {
		return
	}

	e.promote()
}

// Connect to remote elector
func (e *msElector) connectRemoteElector() (*grpc.ClientConn, error) {
	logrus.Infof("[master-slave] [%s] --> try to connect remote elector[%s] ", e.Info().String(), e.remote)

	conn, err := grpc.Dial(
		e.remote,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(1*time.Second),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 30 * time.Second}))

	if err != nil {
		logrus.Warnf("[master-slave] [%s] connect remote elector failed, reason: %v", e.Info().String(), err)
		return nil, err
	}

	logrus.Infof("[master-slave] [%s] connect success", e.Info().String())
	return conn, nil
}

// Do the real shit to connect with the remote election server
// Should be registered as the connect function of connection by a layer of wrapper
func (e *msElector) doRemoteConnect() error {
	connC := make(chan *grpc.ClientConn)

	// FIXME: 这里使用  pingPeriod 作为重连时间间隔是否合适
	ticker := time.NewTicker(time.Duration(e.options.pingPeriod) * time.Second)
	timeout := time.After(time.Duration(e.options.connTimeout) * time.Second)

	once := new(sync.Once)

	for {
		select {
		case <-timeout:
			logrus.Warnf("[master-slave] [%s] connect timeout after [%d]s", e.Info().String(), e.options.connTimeout)
			return errors.New("connect timeout")

		case c := <-connC:
			e.clientConn = c
			e.eleCli = pb.NewElectorClient(c)
			return nil

		case <-ticker.C:
			go func() {
				c, err := e.connectRemoteElector()
				if err == nil {
					once.Do(func() {
						ticker.Stop()
						connC <- c
					})
				}
			}()
		}
	}

	return errors.New("connect failed")
}

// Do the real shit to disconnect with the remote election server
// Should be registered as the close function of connection by a layer of wrapper
func (e *msElector) doRemoteDisconnect() error {
	if e.clientConn != nil {
		return e.clientConn.Close()
	}
	return nil
}

// start elector server in master-slave mode
func (e *msElector) launchElector() error {
	l, err := net.Listen("tcp", e.local)
	if err != nil {
		logrus.Warnf("[master-slave]", err)
		return err
	}

	srv := grpc.NewServer(
		grpc.KeepaliveParams(
			keepalive.ServerParameters{
				Time:    30 * time.Second,
				Timeout: 30 * time.Second,
			},
		),
	)
	pb.RegisterElectorServer(srv, &eleSrv{e})

	e.eleSrvLnr = l
	e.eleSrv = srv

	// FIXME: no error process, is it ok?
	go e.eleSrv.Serve(e.eleSrvLnr)

	return nil
}

// Change elector's role from one to another, at a specific epoch
func (e *msElector) changeRole(from, to Role, epoch uint64) {
	if e.role == to || e.role != from {
		return
	}

	e.role = to
	e.epoch = epoch
	e.count = 0

	saveState(e.stFile, e.role, e.epoch)
	logrus.Infof("[%s] role changed from %s to %s at epoch %d", e.Info().String(), from.String(), to.String(), epoch)
}

func (e *msElector) nextEpoch() uint64 {
	return e.epoch + 1
}

// Ping remote elector
func (e *msElector) ping() {
	if e.role != RoleLeader {
		logrus.Debugf("[master-slave] ping failed, reason: only Leader can ping remote, role => [%s]", e.role)
		return
	}

	if e.eleCli == nil {
		logrus.Debug("[master-slave] ping failed, reason: grpc client to remote elector => [nil]")
		return
	}

	if e.handler.connState == connStateConnecting {
		logrus.Debug("[master-slave] ping failed, reason: in [connStateConnecting]")
		return
	}

	if (e.state & stateRunning) == 0 {
		logrus.Debug("[master-slave] ping failed, reason: not in [stateRunning]")
		return
	}

	/*
		if (e.state&stateRunning) == 0 || e.role != RoleLeader || e.eleCli == nil || e.handler.connState == connStateConnecting {
			logrus.Debugf("[master-slave] [%s] refuse ping cause: s.state=%x, e.role=%v, e.eleCli=%v, e.connState=%s",
				e.Info().String(), e.state, e.role, e.eleCli, e.handler.state().String())
			return
		}
	*/

	var ping pb.MsgPING

	ping.Id, ping.Role, ping.Epoch, ping.Count = e.id, pb.EnumRole(e.role), e.epoch, e.count

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.connTimeout))
	defer cancel()
	r, err := e.eleCli.PING(ctx, &ping)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.handler.connect(e); err != nil {
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
func (e *msElector) seekVote() {
	if (e.state&stateRunning) == 0 || e.role != RoleCandidate || e.eleCli == nil || e.handler.connState == connStateConnecting {
		logrus.Debugf("[%s] refuse seekVote cause: s.state=%x, e.role=%v, e.eleCli=%v, e.connState=%s",
			e.Info().String(), e.state, e.role, e.eleCli, e.handler.connState.String())
		return
	}

	var seekVote pb.MsgSeekVote
	seekVote.Id, seekVote.Role, seekVote.Epoch, seekVote.Bid = e.id, pb.EnumRole(e.role), e.epoch, e.bid

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.connTimeout))
	defer cancel()
	vote, err := e.eleCli.SeekVote(ctx, &seekVote)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.handler.connect(e); err != nil {
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
func (e *msElector) abdicate() {
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

	if e.handler.connState == connStateConnecting {
		logrus.Debugf("[%s] skip sending abdicate user request cause connecting to the other side elector",
			e.Info().String())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.options.connTimeout))
	defer cancel()
	promoted, err := e.eleCli.Abdicate(ctx, &abdicate)
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable:
			if err := e.handler.connect(e); err != nil {
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
func (e *msElector) promote() {
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
func (e *msElector) leaderLoop() {
	var pingTicker = time.NewTicker(time.Duration(e.options.pingPeriod) * time.Second)
	var once = new(sync.Once)

	for (e.state&stateRunning) != 0 && e.role == RoleLeader {
		// leader bootstrap timeout handler
		if (e.state & stateLeaderBootStrapping) != 0 {
			once.Do(func() {
				go time.AfterFunc(time.Duration(e.options.leaderBootstrapPeriod)*time.Second, func() {
					logrus.Debugf("[master-slave] [%s] clear stateLeaderBootStrapping bit cuz timeout", e.Info().String())
					e.state &^= stateLeaderBootStrapping
				})
				logrus.Debugf("[master-slave] [%s] leaderBootstrapTimer has been set", e.Info().String())
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
func (e *msElector) followerLoop() {
	e.timer = time.NewTimer(time.Duration(e.options.leaderTimeoutThreshold) * time.Second)

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
					saveState(e.stFile, e.role, e.epoch)
				}

				if !e.timer.Stop() {
					<-e.timer.C
				}
				e.timer.Reset(time.Duration(e.options.leaderTimeoutThreshold) * time.Second)

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
func (e *msElector) candidateLoop() {
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
					if vev.Role == pb.EnumRole_Leader {
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
	e *msElector
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

	from, expect = strings.Split(pr.Addr.String(), ":")[0], strings.Split(es.e.remote, ":")[0]
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
