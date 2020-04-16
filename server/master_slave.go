package server

import (
	"errors"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	pb "github.com/moooofly/dms-elector/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const defaultTimeout = 1 * time.Second
const defaultRetryPeriod = 3 * time.Second

// for internal communication only
type eEvent struct {
	event interface{} // event into elector
	reply interface{} // event out, maybe nil
	errCh chan error  // err chan, used as event handled signal as well
}

// msElector is the elector used in master-slave mode
type msElector struct {
	id     uint64 // elector id
	role   Role   // elector role
	epoch  uint64 // elector current epoch
	stFile string // state file with role and epoch
	local  string // local elector listening address
	remote string // remote elector listening address

	rs      *roleService
	options electorOptions

	count uint64 // ping counter as a leader
	mu    sync.RWMutex

	started bool
	stopped bool

	grpcClientConn *grpc.ClientConn // client connection to remote elector
	electorClient  pb.ElectorClient // grpc client to remote elector

	startOnce sync.Once

	stopCh         chan bool
	disconnectedCh chan bool
	connectedCh    chan bool

	evCh          chan *eEvent
	userRequestCh chan *eEvent

	backgroundConnectionDoneCh chan bool

	lastConnectErrPtr unsafe.Pointer
}

// NewmsElector is the constructor of msElector
func NewMasterSlave(
	stfile string,
	rsTcpHost, rsUnixPath string,
	local, remote string,
	opts ...electorOption,
) *msElector {

	role, epoch := loadState(stfile)
	return newMasterSlaveWithInfo(stfile, role, epoch, rsTcpHost, rsUnixPath, local, remote, opts...)
}

func newMasterSlaveWithInfo(
	stfile string,
	role Role,
	epoch uint64,
	rsTcpHost, rsUnixPath string,
	local, remote string,
	opts ...electorOption,
) *msElector {

	ms := new(msElector)

	ms.id = rand.Uint64()
	ms.role = role
	ms.epoch = epoch
	ms.stFile = stfile
	ms.local = local
	ms.remote = remote

	ms.rs = newRoleService(rsTcpHost, rsUnixPath, ms)
	ms.count = 0

	// TODO: set default values in an appropriate way
	ms.options.retryPeriod = 3
	ms.options.leaderTimeout = 15
	ms.options.pingPeriod = 2

	for _, o := range opts {
		o(&ms.options)
	}

	return ms
}

// Role gets the role of the elector
func (e *msElector) Role() Role {
	return e.role
}

// Info gets metadata of the elector
func (e *msElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Abdicate yields leadership to another elector
func (e *msElector) Abdicate() {

	// NOTE: we should make local elector abdicate even if
	//       the connection to remote elector is lost

	// Step 1: abdicate from leader to follower locally
	defer func() {
		e.userRequestCh <- &eEvent{userRequestAbdicate, nil, nil}
	}()

	// Step 2: tell remote elector what happens here
	abdicate := pb.MsgAbdicate{
		Id:    e.id,
		Role:  pb.EnumRole(e.role),
		Epoch: e.epoch,
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// tell remote elector about abdicating action
	abdicateRsp, err := e.electorClient.Abdicate(ctx, &abdicate)
	if err != nil {
		logrus.Warnf("[%s] --> send [Abdicate] to remote elector failed, reason: %v", e.Role().String(), err)
		e.setStateDisconnected(err)
		return
	}

	logrus.Infof("[%s] --> send [Abdicate] to remote elector => [%s]", e.Role().String(), abdicate.String())

	e.evCh <- &eEvent{abdicateRsp, nil, nil}
}

// Promote myself to Leader
func (e *msElector) Promote() {
	if e.role == RoleLeader {
		logrus.Warnf("[%s] promote failed, reason: Leader need not to promote", e.Role().String())
		return
	}

	e.userRequestCh <- &eEvent{userRequestPromote, nil, nil}

	logrus.Infof("[%s] make [%s] to promote locally", e.Role().String(), e.Role().String())
}

// Stop shuts down all the connections and resources
// related to the elector.
func (e *msElector) Stop() error {
	logrus.Infof("[master-slave] stop elector as [%s]", e.Role().String())

	e.mu.RLock()
	cc := e.grpcClientConn
	started := e.started
	stopped := e.stopped
	e.mu.RUnlock()

	if !started {
		return errors.New("not started")
	}
	if stopped {
		return nil
	}

	// Now close the underlying gRPC connection.
	var err error
	if cc != nil {
		err = cc.Close()
	}

	// At this point we can change the state variables: started and stopped
	e.mu.Lock()
	e.started = false
	e.stopped = true
	e.mu.Unlock()

	close(e.stopCh)

	// Ensure that the backgroundConnector returns
	<-e.backgroundConnectionDoneCh

	if e.rs != nil {
		e.rs.Stop()
	}

	saveState(e.stFile, e.role, e.epoch)

	return err
}

// Start dials to the remote elector, establishing a connection to it.
// it invokes a background connector that will reattempt connections
// to the remote elector periodically if the connection dies.
func (e *msElector) Start() (err error) {
	err = errors.New("already started")

	e.startOnce.Do(func() {
		e.mu.Lock()
		e.started = true
		e.disconnectedCh = make(chan bool, 1)
		e.connectedCh = make(chan bool, 1)
		e.stopCh = make(chan bool)

		// FIXME:
		e.evCh = make(chan *eEvent, 1024)
		e.userRequestCh = make(chan *eEvent, 100)

		e.backgroundConnectionDoneCh = make(chan bool)
		e.mu.Unlock()

		// step 1: 启动 elector server
		if err = e.launchElector(); err != nil {
			// FIXME: 直接使用 Fatalf ?
			logrus.Warnf("[master-slave] launch elector as [%s] at [%s] failed, reason: %v",
				e.Role().String(), e.local, err)
		}
		logrus.Infof("[master-slave] launch elector as [%s] at [%s] success", e.Role().String(), e.local)

		// step 3: 和 remote elector 建立连接
		// An optimistic first connection attempt to ensure that applications
		// under heavy load can immediately process data.
		if err = e.connect(); err == nil {
			e.setStateConnected()
		} else {
			e.setStateDisconnected(err)
		}

		go e.indefiniteBackgroundConnection()
		go e.mainLoop()

		// step 2: 启动 role service
		if err = e.rs.Start(); err != nil {
			// FIXME: 直接使用 Fatalf ?
			logrus.Warnf("[master-slave] start grpc-role-service failed, reason: %v", err)
		}

	})

	return err
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
	pb.RegisterElectorServer(srv, &electorService{e})

	// FIXME: no error process, is it ok?
	go srv.Serve(l)

	return nil
}

func (e *msElector) connect() error {
	cc, err := e.connectRemoteElector()
	if err != nil {
		return err
	}

	return e.enableConnectionStreams(cc)
}

// Connect to remote elector
func (e *msElector) connectRemoteElector() (*grpc.ClientConn, error) {
	logrus.Infof("[%s] --> try to connect remote elector[%s] ", e.Role().String(), e.remote)

	conn, err := grpc.Dial(
		e.remote,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(defaultTimeout),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 30 * time.Second}))

	if err != nil {
		logrus.Warnf("[%s]     connect remote elector failed, reason: %v", e.Role().String(), err)
		return nil, err
	}

	logrus.Infof("[%s]     connect success", e.Role().String())
	return conn, nil
}

func (e *msElector) enableConnectionStreams(cc *grpc.ClientConn) error {
	e.mu.RLock()
	started := e.started
	e.mu.RUnlock()

	if !started {
		return errors.New("not started")
	}

	e.mu.Lock()
	// If the previous clientConn was non-nil, close it
	if e.grpcClientConn != nil {
		_ = e.grpcClientConn.Close()
	}
	e.grpcClientConn = cc
	e.electorClient = pb.NewElectorClient(cc)
	e.mu.Unlock()

	return nil
}

func (e *msElector) mainLoop() error {

	switch e.role {
	case RoleFollower:
		e.followerLoop()
	case RoleLeader:
		e.leaderLoop()
	default:
		logrus.Warnf("[master-slave] [%s] is not a legal role", e.Role().String())
		return errors.New("not a legal role")
	}

	return nil
}

/*
Leader mainloop:

1. ping the other side;
2. handle user request: abdicate;
3. handle received message:
	1) PING: brain-split detected, change to follower if:
		a. my epoch is smaller;
		b. the epochs are equal, but my ping counter is smaller.
	2) PONG: change to follower if the [role:epoch:count] in PONG message says the other side is THE right leader;
	3) SeekVote: reject;
	4) Vote: nothing to do, ignore;
	5) Abdicate: nothing to do in fact, but reply anyway;
	6) Promoted: nothing to do.
*/
func (e *msElector) leaderLoop() {

	logrus.Debug("----------->  in [[  leader  ]] loop")
	defer logrus.Debug("<----------- out [[  leader  ]] loop")

	for {

		select {
		case <-e.stopCh:
			return

		case userReq := <-e.userRequestCh:
			if abdicateReq := userReq.event.(userRequest); abdicateReq == userRequestAbdicate {

				logrus.Infof("[%s] abdicate myself from [Leader] to [Follower]", e.Role().String())

				e.changeRole(RoleLeader, RoleFollower, e.epoch)

				go e.followerLoop()
				e.setStateConnected()

				return
			}

		case <-e.connectedCh:

			ticker := time.NewTicker(time.Duration(e.options.pingPeriod) * time.Second)
			defer ticker.Stop()

			for {

				if !e.connected() {
					break
				}

				select {
				case <-ticker.C:
					e.count++
					e.ping()

				case eev := <-e.evCh:
					ev := eev.event

					switch ev.(type) {
					case *pb.MsgPING:
						remoteEv := ev.(*pb.MsgPING)

						// brain-split recovery
						if e.epoch < remoteEv.Epoch || (e.epoch == remoteEv.Epoch && e.count < remoteEv.Count) {

							logrus.Infof("[%s] <-- recv [Ping] from an elder leader, changing to [Follower]",
								e.Role().String())

							e.changeRole(RoleLeader, RoleFollower, remoteEv.Epoch)

							eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: e.count}
							eev.errCh <- nil

							go e.followerLoop()
							e.setStateConnected()

							return
						} else {
							logrus.Infof("[%s] <-- recv [Ping] from a younger leader, send a [Pong] back",
								e.Role().String())

							eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: e.count}
							eev.errCh <- nil
						}

					case *pb.MsgPONG:
						remoteEv := ev.(*pb.MsgPONG)

						// check if the other side is a good leader
						if e.epoch < remoteEv.Epoch || (e.epoch == remoteEv.Epoch && e.count < remoteEv.Count) {

							logrus.Infof("[%s] <-- recv [Pong] from an elder leader, changing to [Follower]",
								e.Role().String())

							e.changeRole(RoleLeader, RoleFollower, remoteEv.Epoch)

							go e.followerLoop()
							e.setStateConnected()

							return
						} else {
							logrus.Debugf("[%s] <-- recv [Pong] => [%s]", e.Role().String(), remoteEv.String())
						}

					case *pb.MsgSeekVote:
						// refuse
						eev.reply = pb.MsgVote{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Agreed: false}
						eev.errCh <- nil

					case *pb.MsgVote:
						// nothing to do

					case *pb.MsgAbdicate:
						// NOTE: 回应 remote 的消息，但由于 local elector 已经是 leader ，因此无需 promote
						logrus.Infof("[%s] <-- recv [Abdicate] from remote elector (claimed Leader), send [Promoted: false] back", e.Role().String())

						eev.reply = pb.MsgPromoted{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Promoted: false}
						eev.errCh <- nil

					case *pb.MsgPromoted:
						// nothing to do

					default:
						logrus.Warnf("[%s] not a good event: %T", e.Info().String(), ev)
					}

				}
			}
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
	logrus.Debug("----------->  in [[ follower ]] loop")
	defer logrus.Debug("<----------- out [[ follower ]] loop")

	for {

		select {
		case <-e.stopCh:
			return

		case userReq := <-e.userRequestCh:
			if promoteReq := userReq.event.(userRequest); promoteReq == userRequestPromote {
				logrus.Infof("[%s] <-- recv [Promote] by user request, promote myself to Leader",
					e.Role().String())

				e.changeRole(e.role, RoleLeader, e.nextEpoch())

				go e.leaderLoop()
				e.setStateConnected()

				return
			}

		case <-e.connectedCh:

			ticker := time.NewTimer(time.Duration(e.options.leaderTimeout) * time.Second)
			defer ticker.Stop()

			for {

				if !e.connected() {
					break
				}

				select {
				case <-ticker.C:
					logrus.Infof("[%s] lost conection to Leader, more than [%d]s, promote myself to [Leader]",
						e.Role().String(), e.options.leaderTimeout)

					e.changeRole(RoleFollower, RoleLeader, e.nextEpoch())

					go e.leaderLoop()
					e.setStateConnected()

					return

				case eev := <-e.evCh:
					ev := eev.event

					switch ev.(type) {
					case *pb.MsgPING:
						remoteEv := ev.(*pb.MsgPING)

						logrus.Infof("[%s] <-- recv [Ping, count:%d], send [Pong] back",
							e.Role().String(), remoteEv.Count)

						if e.epoch != remoteEv.Epoch {
							e.epoch = remoteEv.Epoch
							saveState(e.stFile, e.role, e.epoch)
						}

						if !ticker.Stop() {
							<-ticker.C
						}
						ticker.Reset(time.Duration(e.options.leaderTimeout) * time.Second)

						// NOTE: follower should reply with the same count as the ping
						eev.reply = pb.MsgPONG{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Count: remoteEv.Count}
						eev.errCh <- nil

					case *pb.MsgPONG:
						// NOTE: follower has no right to ping, should never receive [Pong]

					case *pb.MsgSeekVote:
						// NOTE:
						// 1. follower has no right to vote, just refuse it
						// 2. after refactoring, follower should never receive [SeekVote] again
						eev.reply = pb.MsgVote{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Agreed: false}
						eev.errCh <- nil

					case *pb.MsgVote:
						// NOTE: follower has no right to vote, should never receive [Vote]

					case *pb.MsgAbdicate:
						logrus.Infof("[%s] <-- recv [Abdicate] from remote elector (Leader), send [Promoted: true] back", e.Role().String())

						e.changeRole(RoleFollower, RoleLeader, e.nextEpoch())

						eev.reply = pb.MsgPromoted{Id: e.id, Role: pb.EnumRole(e.role), Epoch: e.epoch, Promoted: true}
						eev.errCh <- nil

						go e.leaderLoop()
						e.setStateConnected()

						return

					case *pb.MsgPromoted:
						logrus.Infof("[%s] <-- recv [Promoted] from a new Leader just being promoted", e.Role().String())

					default:
						logrus.Debugf("not a good event: %T", ev)
					}
				}
			}

		}
	}
}

func (e *msElector) lastConnectError() error {
	errPtr := (*error)(atomic.LoadPointer(&e.lastConnectErrPtr))
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

func (e *msElector) saveLastConnectError(err error) {
	var errPtr *error
	if err != nil {
		errPtr = &err
	}
	atomic.StorePointer(&e.lastConnectErrPtr, unsafe.Pointer(errPtr))
}

func (e *msElector) setStateDisconnected(err error) {
	e.saveLastConnectError(err)

	select {
	case e.disconnectedCh <- true:
	default:
	}
}

func (e *msElector) setStateConnected() {
	e.saveLastConnectError(nil)

	select {
	case e.connectedCh <- true:
	default:
	}
}

func (e *msElector) connected() bool {
	return e.lastConnectError() == nil
}

func (e *msElector) indefiniteBackgroundConnection() error {
	defer func() {
		e.backgroundConnectionDoneCh <- true
	}()

	retry := time.Duration(e.options.retryPeriod) * time.Second
	if retry <= 0 {
		retry = defaultRetryPeriod
	}

	// No strong seeding required, nano time can
	// already help with pseudo uniqueness.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63n(1024)))

	// maxJitter: 1 + (70% of the retryPeriod)
	maxJitter := int64(1 + 0.7*float64(retry))

	for {
		// Otherwise these will be the normal scenarios to enable
		// reconnections if we trip out.
		// 1. If we've stopped, return entirely
		// 2. Otherwise block until we are disconnected, and
		//    then retry connecting
		select {
		case <-e.stopCh:
			return errors.New("stopped")

		case <-e.disconnectedCh:
			// Normal scenario that we'll wait for
		}

		if err := e.connect(); err == nil {
			e.setStateConnected()
		} else {
			e.setStateDisconnected(err)
		}

		// Apply some jitter to avoid lockstep retrials of other
		// agent-exporters. Lockstep retrials could result in an
		// innocent DDOS, by clogging the machine's resources and network.
		jitter := time.Duration(rng.Int63n(maxJitter))

		select {
		case <-e.stopCh:
			return errors.New("stopped")

		case <-time.After(retry + jitter):
		}
	}
}

// Change elector's role from one to another, at a specific epoch
func (e *msElector) changeRole(from, to Role, epoch uint64) {
	if e.role == to || e.role != from {
		return
	}

	logrus.Infof("[%s] change role from [%s] to [%s] at epoch [%d]",
		e.Role().String(), from.String(), to.String(), epoch)

	e.role = to
	e.epoch = epoch
	e.count = 0

	saveState(e.stFile, e.role, e.epoch)
}

func (e *msElector) nextEpoch() uint64 {
	return e.epoch + 1
}

// -----------------------

// grpc client API

// Ping remote elector
// NOTE: only master can ping remote
func (e *msElector) ping() {
	if e.role != RoleLeader {
		logrus.Warnf("[%s] ping failed, reason: only Leader can ping remote", e.Role().String())
		return
	}

	ping := pb.MsgPING{
		Id:    e.id,
		Role:  pb.EnumRole(e.role),
		Epoch: e.epoch,
		Count: e.count,
	}

	// FIXME: better setting
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	rsp, err := e.electorClient.PING(ctx, &ping)
	if err != nil {
		logrus.Warnf("[%s] --> send [Ping] failed, reason: %v", e.Role().String(), err)
		e.setStateDisconnected(err)
		return
	}

	logrus.Debugf("[%s] --> send [Ping] => [%s]", e.Role().String(), ping.String())
	logrus.Infof("[%s] --> send [Ping, count:%d], recv [Pong] back", e.Role().String(), e.count)

	e.evCh <- &eEvent{rsp, nil, nil}
}

// gRPC server callback

type electorService struct {
	e *msElector
}

func (es *electorService) PING(ctx context.Context, ping *pb.MsgPING) (*pb.MsgPONG, error) {
	var eev eEvent

	logrus.Debugf("[%s] <-- recv [Ping] => [%s]", es.e.Role().String(), ping.String())

	if err := es.sanityCheck(ctx); err != nil {
		logrus.Warnf("[%s] sanityCheck failed, reason: %v", es.e.Role().String(), err)
		return nil, err
	}

	eev.event = ping
	eev.errCh = make(chan error)

	es.e.evCh <- &eev

	if err := <-eev.errCh; err != nil {
		s, _ := status.FromError(err)
		logrus.Warnf("[%s] error: %v", es.e.Role().String(), err)
		return nil, s.Err()
	}

	reply := eev.reply.(pb.MsgPONG)

	logrus.Debugf("[%s] --> send [Pong] => [%s]", es.e.Role().String(), reply.String())
	return &reply, nil
}

func (es *electorService) SeekVote(ctx context.Context, seek *pb.MsgSeekVote) (*pb.MsgVote, error) {
	var eev eEvent

	logrus.Debugf("[%s] <-- recv [SeekVote] => [%s]", es.e.Role().String(), seek.String())

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

	logrus.Debugf("[%s] --> send [Vote] => [%s]", es.e.Role().String(), reply.String())
	return &reply, nil
}

func (es *electorService) Abdicate(ctx context.Context, abdicate *pb.MsgAbdicate) (*pb.MsgPromoted, error) {
	var eev eEvent

	logrus.Debugf("[%s] <-- recv [Abdicate] => [%s]", es.e.Role().String(), abdicate.String())

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

	logrus.Debugf("[%s] --> send [Promoted] => [%s]", es.e.Role().String(), reply.String())
	return &reply, nil
}

func (es *electorService) sanityCheck(ctx context.Context) error {
	var remote *peer.Peer
	var ok bool

	if remote, ok = peer.FromContext(ctx); !ok {
		return status.Error(codes.DataLoss, "failed to get peer from ctx")
	}
	if remote.Addr == net.Addr(nil) {
		return status.Error(codes.DataLoss, "failed to get peer address")
	}

	from, expect := strings.Split(remote.Addr.String(), ":")[0], strings.Split(es.e.remote, ":")[0]
	if from != expect {
		logrus.Warnf("[%s] refuse this ping, reason: from [%s], not from [%s] as expect",
			es.e.Role().String(), from, expect)
		return errors.New("wrong target to connect")
	}

	return nil
}
