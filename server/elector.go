package server

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moooofly/dms-elector/pkg/util"

	pb "github.com/moooofly/dms-elector/proto"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////
// Connection
////////////////////////////////////////////////////////////////////////////
type connState uint

const (
	connStateDisconnect connState = iota
	connStateConnecting
	connStateConnected
)

var connState2Str = map[connState]string{
	connStateDisconnect: "DISCONNECT",
	connStateConnecting: "CONNECTING",
	connStateConnected:  "CONNECTED",
}

func (cs connState) String() string {
	return connState2Str[cs]
}

type connFunc func(...interface{}) error
type closeFunc func(...interface{}) error

// NOTE: fd, 20180705
// connHandler is nothing but a hub of connect/close functions with a non-specific remote target,
// it's main usage is to handle reconnect operations, while making sure that only one call is firing
// even if there are multi. User should register their connect/close function to it, and use
// connect/close function of connHandler when doing shit.
type connHandler struct {
	sync.Mutex
	connState connState

	connectF connFunc
	closeF   closeFunc
}

func (conn *connHandler) registerConnFunc(f func(...interface{}) error) {
	conn.connectF = f
}

func (conn *connHandler) registerCloseFunc(f func(...interface{}) error) {
	conn.closeF = f
}

func (conn *connHandler) connect(args ...interface{}) error {
	var err error

	if conn.connectF == nil {
		err = errors.New("connect function not registered")
		return err
	}

	conn.Mutex.Lock()
	if conn.connState == connStateConnecting {
		conn.Mutex.Unlock()
		return errors.New("is already connecting")
	}

	conn.connState = connStateConnecting
	conn.Mutex.Unlock()

	err = conn.connectF(args...)

	conn.Mutex.Lock()
	if err == nil {
		conn.connState = connStateConnected
	} else {
		conn.connState = connStateDisconnect
	}
	conn.Mutex.Unlock()

	return err
}

func (conn *connHandler) close(args ...interface{}) error {
	var err error

	if conn.closeF == nil {
		err = errors.New("close function not registered")
		return err
	}

	conn.Mutex.Lock()
	conn.connState = connStateDisconnect
	conn.Mutex.Unlock()

	return conn.closeF(args...)
}

func (conn *connHandler) state() connState {
	conn.Mutex.Lock()
	defer conn.Mutex.Unlock()
	return conn.connState
}

////////////////////////////////////////////////////////////////////////////
// Roles
////////////////////////////////////////////////////////////////////////////

// Role represents the role of the elector
type Role uint

const (
	// RoleCandidate represents a candidate
	RoleCandidate = Role(0)

	// RoleFollower represents a follower
	RoleFollower = Role(1)

	// RoleLeader represents a leader
	RoleLeader = Role(2)

	// RoleUnstable represents an unstable elector, which indicate that the user who request
	// the elector's role may retry later.
	// NOTE: the RoleUnstable is NOT a role of elector-state-machine, it is only used
	// for replying user requests when the leader is in leaderBootstrapState state.
	RoleUnstable = Role(5)
)

func (r Role) String() string {
	switch r {
	case RoleCandidate:
		return "Candidate"
	case RoleFollower:
		return "Follower"
	case RoleLeader:
		return "Leader"
	case RoleUnstable:
		return "Unstable"
	default:
		return "Unknown"
	}
}

// StringShort return a short-form of String()
func (r Role) StringShort() string {
	switch r {
	case RoleCandidate:
		return "C"
	case RoleFollower:
		return "F"
	case RoleLeader:
		return "L"
	case RoleUnstable:
		return "U"
	default:
		return "X"
	}
}

////////////////////////////////////////////////////////////////////////////
// Electors
////////////////////////////////////////////////////////////////////////////
type electorState uint

const (
	stateStopped = 0x0000
	stateRunning = 0x1000

	stateLeaderBootStrapping = 0x0100
	stateRoleChanging        = 0x0010
	stateAbdicating          = stateRoleChanging | 0x0001
	statePromoting           = stateRoleChanging | 0x0002
)

type userRequest uint

const (
	userRequestAbdicate userRequest = iota
	userRequestPromote
)

// Elector is the interface of all kinds of electors
type Elector interface {
	Start() error

	Stop()

	Info() ElectorInfo

	Abdicate()

	Promote()
}

// ElectorInfo represent basic info for a elector
type ElectorInfo struct {
	id    uint64
	role  Role
	epoch uint64
}

func (ebi ElectorInfo) String() string {
	return fmt.Sprintf("0x%x, %s, 0x%x", ebi.id, ebi.role.StringShort(), ebi.epoch)
}

type electorOptions struct {
	/************************* MS mode options *************************/
	eleConnTimeout uint // connection timeout
	seekVotePeriod uint // seek vote period
	seekVoteMaxTry uint // seek vote max try
	leaderTimeout  uint // leader timeout
	pingPeriod     uint // ping period

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

// ElectorOption config how the elector works
type ElectorOption func(o *electorOptions)

// WithLeaderBootStrapPeriod set the period among sending ping
func WithLeaderBootStrapPeriod(period uint) ElectorOption {
	return func(o *electorOptions) {
		o.leaderBootstrapPeriod = period
	}
}

// WithEleConnTimeout set the timeout for connect remote elector server
func WithEleConnTimeout(timeout uint) ElectorOption {
	return func(o *electorOptions) {
		o.eleConnTimeout = timeout
	}
}

// WithSeekVotePeriod set the period among seeking vote
func WithSeekVotePeriod(period uint) ElectorOption {
	return func(o *electorOptions) {
		o.seekVotePeriod = period
	}
}

// WithSeekVoteMaxTry set the max time for seeking vote
func WithSeekVoteMaxTry(try uint) ElectorOption {
	return func(o *electorOptions) {
		o.seekVoteMaxTry = try
	}
}

// WithLeaderTimeout set how long we thought the leader is unreachable
func WithLeaderTimeout(timeout uint) ElectorOption {
	return func(o *electorOptions) {
		o.leaderTimeout = timeout
	}
}

// WithPingPeriod set the period among sending ping
func WithPingPeriod(period uint) ElectorOption {
	return func(o *electorOptions) {
		o.pingPeriod = period
	}
}

// WithProtectionPeriod set the period of leader protection
func WithProtectionPeriod(period uint) ElectorOption {
	return func(o *electorOptions) {
		o.protectionPeriod = period
	}
}

/********** Single point elector **********/

// SinglePointElector is the elector when running in single point mode,
// it has nothing meaningful in fact cuz no need to build a leader-follower relationship,
// but used as well...
// NOTE:
type SinglePointElector struct {
	id    uint64
	role  Role
	epoch uint64

	status electorState

	path string // persistence file path

	reqSrv *requestServer // user request server
}

// NewSinglePointElector is the constructor of SinglePointElector
func NewSinglePointElector(path string, reqSrvHost, reqSrvPath string) *SinglePointElector {
	role, _ := readPersistenceFile(path)
	return newSinglePointElectorWithInfo(path, role, reqSrvHost, reqSrvPath)
}

func newSinglePointElectorWithInfo(path string, role Role, reqSrvHost, reqSrvPath string) *SinglePointElector {
	ele := new(SinglePointElector)

	ele.id = rand.Uint64()
	ele.role = role
	ele.epoch = 0
	ele.status = stateStopped
	ele.path = path
	ele.reqSrv = newRequestServer(reqSrvHost, reqSrvPath, ele)

	return ele
}

// Info the elector
func (e *SinglePointElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start the elector
func (e *SinglePointElector) Start() error {
	e.status = stateRunning
	// NOTE: fd, 20180511
	// always start as a leader in single-point mode
	e.changeRole(e.role, RoleLeader)

	if e.reqSrv.host != "" || e.reqSrv.path != "" {
		if err := e.reqSrv.start(); err != nil {
			logrus.Warnf("[%s] Cannot start local request server: %v", e.Info().String(), err)
			return err
		}
	} else {
		logrus.Warnf("[%s] Request server disabled", e.Info().String())
	}

	return nil
}

// Stop the elector
func (e *SinglePointElector) Stop() {
	e.status = stateStopped
	writePersistenceFile(e.path, e.role, 0)
	e.reqSrv.stop()
}

// Role return the elector's role now, which is useless in fact
func (e *SinglePointElector) Role() Role {
	return e.role
}

// Abdicate the leadership, which is useless in fact
func (e *SinglePointElector) Abdicate() {
	if e.role != RoleLeader {
		return
	}

	e.changeRole(RoleLeader, RoleFollower)
}

// Promote as a leader, which is useless in fact
func (e *SinglePointElector) Promote() {
	if e.role == RoleLeader {
		return
	}

	e.changeRole(e.role, RoleLeader)
}

func (e *SinglePointElector) changeRole(from, to Role) {
	if e.role == to || e.role != from {
		return
	}

	e.role = to

	writePersistenceFile(e.path, e.role, 0)
}

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
func NewMSElector(path string, reqSrvHost, reqSrvPath, localEleHost, remoteEleHost string, opts ...ElectorOption) *MSElector {
	role, epoch := readPersistenceFile(path)
	return newMSElectorWithInfo(path, role, epoch, reqSrvHost, reqSrvPath, localEleHost, remoteEleHost, opts...)
}

func newMSElectorWithInfo(path string, role Role, epoch uint64, reqSrvHost, reqSrvPath, localEleHost, remoteEleHost string, opts ...ElectorOption) *MSElector {
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
	if e.reqSrv.host != "" || e.reqSrv.path != "" {
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
	writePersistenceFile(e.path, e.role, e.epoch)
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

	writePersistenceFile(e.path, e.role, e.epoch)
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
					writePersistenceFile(e.path, e.role, e.epoch)
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
func NewClusterElector(path, reqSrvHost, reqSrvPath string, zkHost []string, zkLeaderDir string, opts ...ElectorOption) *ClusterElector {
	role, _ := readPersistenceFile(path)
	return newClusterElectorWithInfo(path, role, reqSrvHost, reqSrvPath, zkHost, zkLeaderDir, opts...)
}

func newClusterElectorWithInfo(path string, role Role, reqSrvHost, reqSrvPath string, zkHost []string, zkLeaderDir string, opts ...ElectorOption) *ClusterElector {
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
	if e.reqSrv.host != "" || e.reqSrv.path != "" {
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
func (e *ClusterElector) Stop() {
	logrus.Infof("[%d] stopping", e.id)
	e.state = stateStopped
	close(e.stopCh)
	e.zkConnHandler.close(e)
	if e.reqSrv != nil {
		e.reqSrv.stop()
	}
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
	writePersistenceFile(e.path, e.role, 0)
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

////////////////////////////////////////////////////////////////////////////
// User request server
////////////////////////////////////////////////////////////////////////////

/*
Protocol: (version 0.2)


1. Request:

request = '?' (req_role | req_abdicate | req_promote) reserved_padding ';'
req_role = '0'
req_abdicate = '1'
req_promote = '2'
reserved_padding = '0'

2. Reply:

reply = '!' (ack | nack) content ';'
ack = '1'
nack = '0'
content = rpl_role | padding
rpl_role = r_candidate | r_follower | r_leader | r_unstable
r_candidate =  '0'
r_follower = '1'
r_leader = '2'
r_unstable = '5'
padding = '0'

*/
const (
	usrReqRole     = '0'
	usrReqAbdicate = '1'
	usrReqPromote  = '2'

	usrRplRoleCandidate = '0'
	usrRplRoleFollower  = '1'
	usrRplRoleLeader    = '2'
	usrRplRoleUnstable  = '5'

	usrRplAck     = '1'
	usrRplNack    = '0'
	usrRplPadding = '0'
)

const (
	// in second
	reqSrvReadTimeout  = 30
	reqSrvWriteTimeout = 30
)

var role2reply = map[Role]rune{
	RoleCandidate: usrRplRoleCandidate,
	RoleFollower:  usrRplRoleFollower,
	RoleLeader:    usrRplRoleLeader,
	RoleUnstable:  usrRplRoleUnstable}

/****** User request server ******/
type requestServer struct {
	// NOTE: fd, 20180831
	// we support unix domain server now, to decrease TIME_WAIT hanging sockets
	host string
	path string

	e Elector

	lnrs []net.Listener

	stopCh chan struct{}
}

func newRequestServer(host, path string, e Elector) *requestServer {
	return &requestServer{host: host, path: path, e: e}
}

func (rs *requestServer) handler(conn net.Conn) {
	for {
		select {
		case <-rs.stopCh:
			conn.Close()
			return

		default:
			var reply []byte
			var buff = make([]byte, 4)

			// read request
			conn.SetReadDeadline(time.Now().Add(time.Second * reqSrvReadTimeout))
			_, err := conn.Read(buff)
			if err != nil {
				if err == io.EOF {
					logrus.Debugf("[req] %v close connection", conn.RemoteAddr())
				} else {
					logrus.Warnf("[req] err for %v: %v", conn.RemoteAddr(), err)
				}
				conn.Close()
				return
			}

			if buff[0] != '?' || buff[3] != ';' {
				logrus.Warnf("[req] get a request which cannot be handler: %v from %v", buff, conn.RemoteAddr())
				reply = []byte{'!', usrRplNack, usrRplPadding, ';'}
			} else {
				switch buff[1] {
				case usrReqRole:
					logrus.Debugf("[req] get a request for role from %v", conn.RemoteAddr())
					reply = []byte{'!', usrRplAck, byte(role2reply[rs.e.Info().role]), ';'}

				case usrReqAbdicate:
					logrus.Debugf("[req] get a request for abdicate from %v", conn.RemoteAddr())
					rs.e.Abdicate()
					reply = []byte{'!', usrRplAck, usrRplPadding, ';'}

				case usrReqPromote:
					logrus.Debugf("[req] get a request for promote from %v", conn.RemoteAddr())
					rs.e.Promote()
					reply = []byte{'!', usrRplAck, usrRplPadding, ';'}

				default:
					logrus.Warnf("[req] get a request which cannot be handler: %v from %v", buff, conn.RemoteAddr())
					reply = []byte{'!', usrRplNack, usrRplPadding, ';'}
				}
			}

			logrus.Debugf("[req] reply %v as: %v", conn.RemoteAddr(), string(reply))

			conn.SetWriteDeadline(time.Now().Add(time.Second * reqSrvWriteTimeout))
			_, err = conn.Write(reply)
			if err != nil {
				logrus.Warnf("[req] err for %v: %v", conn.RemoteAddr(), err)
				conn.Close()
				return
			}

			logrus.Debugf("[req] %v replied", conn.RemoteAddr())
		}
	}
}

func (rs *requestServer) start() error {
	var tcpLsnr, unixLsnr net.Listener
	var err error

	rs.stopCh = make(chan struct{})

	if rs.host != "" {
		tcpLsnr, err = net.Listen("tcp", rs.host)
		if err != nil {
			return err
		}
		rs.lnrs = append(rs.lnrs, tcpLsnr)
	}
	if rs.path != "" {
		// remove socket file if exists
		if _, err := os.Stat(rs.path); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else if err = os.Remove(rs.path); err != nil {
			return err
		}
		unixLsnr, err = net.Listen("unix", rs.path)
		if err != nil {
			return err
		}
		rs.lnrs = append(rs.lnrs, unixLsnr)
	}

	for _, l := range rs.lnrs {
		go func(l net.Listener) {
			logrus.Infof("[req] request server online! %v", l)

			for {
				select {
				case <-rs.stopCh:
					return
				default:
					conn, err := l.Accept()
					if err != nil {
						continue
					}
					go rs.handler(conn)
				}
			}
		}(l)
	}

	return nil
}

func (rs *requestServer) stop() {
	if rs.stopCh != nil {
		close(rs.stopCh)
	}
	for _, l := range rs.lnrs {
		l.Close()
	}
}

////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////
func readPersistenceFile(path string) (role Role, epoch uint64) {
	content, err := util.ReadFile(path)
	if err != nil {
		return role, epoch
	}

	for _, c := range content {
		switch c[0] {
		case "Role":
			switch c[1] {
			case "Candidate":
				role = RoleCandidate
			case "Follower":
				role = RoleFollower
			case "Leader":
				role = RoleLeader
			default:
				return RoleCandidate, uint64(0)
			}

		case "Epoch":
			epoch, err = strconv.ParseUint(c[1], 10, 64)
			if err != nil {
				return RoleCandidate, uint64(0)
			}
		}
	}

	return role, epoch
}

func writePersistenceFile(path string, role Role, epoch uint64) error {
	var lines [][]string

	lines = append(lines, []string{"Role", role.String()})
	lines = append(lines, []string{"Epoch", fmt.Sprintf("%d", epoch)})

	return util.WriteFile(path, lines)
}
