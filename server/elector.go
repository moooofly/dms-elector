package server

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/moooofly/dms-elector/pkg/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc/peer"

	cryptRand "crypto/rand"
)

func init() {
	n, err := cryptRand.Int(cryptRand.Reader, big.NewInt(math.MaxInt64))
	var seed int64
	if err != nil {
		seed = time.Now().UnixNano()
	} else {
		seed = n.Int64()
	}

	rand.Seed(seed)
}

type electorOptions struct {
	// options for master-slave mode
	retryPeriod   uint // retry period of re-connect
	pingPeriod    uint // ping period
	leaderTimeout uint // leader timeout

	// options for cluster mode
	protectionPeriod uint // protection period for leader
}

type electorOption func(o *electorOptions)

// WithRetryPeriod set the retry period for re-connection
func WithRetryPeriod(retryPeriod uint) electorOption {
	return func(o *electorOptions) {
		o.retryPeriod = retryPeriod
	}
}

// WithLeaderTimeout set how long we thought the leader is unreachable
func WithLeaderTimeout(timeout uint) electorOption {
	return func(o *electorOptions) {
		o.leaderTimeout = timeout
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
		return errors.New("connect function not registered")
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

// Role represents the role of the elector
type Role int32

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

	stateRoleChanging = 0x0010
	stateAbdicating   = stateRoleChanging | 0x0001
	statePromoting    = stateRoleChanging | 0x0002
)

type userRequest uint

const (
	userRequestAbdicate userRequest = iota
	userRequestPromote
)

// Elector is the interface of all kinds of electors
type Elector interface {
	Start() error
	Stop() error
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
	//return fmt.Sprintf("0x%x, %s, 0x%x", ebi.id, ebi.role.StringShort(), ebi.epoch)
	return fmt.Sprintf("%s, epoch:0x%x", ebi.role.String(), ebi.epoch)
}

func loadState(path string) (role Role, epoch uint64) {
	content, err := util.ReadFile(path)
	if err != nil {
		log.Println(err)
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

func saveState(path string, role Role, epoch uint64) error {
	var lines [][]string

	lines = append(lines, []string{"Role", role.String()})
	lines = append(lines, []string{"Epoch", fmt.Sprintf("%d", epoch)})

	return util.WriteFile(path, lines)
}

func getClietAddr(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("[getClinetIP] invoke FromContext() failed")
	}
	if pr.Addr == net.Addr(nil) {
		return "", fmt.Errorf("[getClientIP] peer.Addr is nil")
	}

	// NOTE: maybe only ip is enough?
	//addSlice := strings.Split(pr.Addr.String(), ":")
	//return addSlice[0], nil

	return pr.Addr.String(), nil
}
