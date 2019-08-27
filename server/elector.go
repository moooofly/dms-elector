package server

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/moooofly/dms-elector/pkg/util"
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

////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////
func loadState(path string) (role Role, epoch uint64) {
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

func saveState(path string, role Role, epoch uint64) error {
	var lines [][]string

	lines = append(lines, []string{"Role", role.String()})
	lines = append(lines, []string{"Epoch", fmt.Sprintf("%d", epoch)})

	return util.WriteFile(path, lines)
}
