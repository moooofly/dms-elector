package server

import (
	"math/rand"

	"github.com/sirupsen/logrus"
)

/********** single point elector **********/

// spElector is the elector when running in single point mode,
// it has nothing meaningful in fact cuz no need to build a leader-follower relationship,
// but used as well...
type spElector struct {
	id    uint64
	role  Role
	epoch uint64

	status electorState

	stFile string // persistence file path

	reqSrv *requestServer // user request server
}

// NewSinglePoint is the constructor of spElector
func NewSinglePoint(stfile, rsTcpHost, rsUnixHost string) *spElector {
	role, _ := loadState(stfile)
	return newSinglePointWithInfo(stfile, role, rsTcpHost, rsUnixHost)
}

func newSinglePointWithInfo(stfile string, role Role, rsTcpHost, rsUnixHost string) *spElector {
	ele := new(spElector)

	ele.id = rand.Uint64()
	ele.role = role
	ele.epoch = 0
	ele.status = stateStopped
	ele.stFile = stfile

	ele.reqSrv = newRequestServer(rsTcpHost, rsUnixHost, ele)

	return ele
}

// Info the elector
func (e *spElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start the elector
func (e *spElector) Start() error {
	e.status = stateRunning
	// NOTE: fd, 20180511
	// always start as a leader in single-point mode
	e.updateRole(e.role, RoleLeader)

	if e.reqSrv.tcpHost != "" || e.reqSrv.unixHost != "" {
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
func (e *spElector) Stop() {
	e.status = stateStopped
	saveState(e.stFile, e.role, 0)
	e.reqSrv.stop()
}

// Role return the elector's role now, which is useless in fact
func (e *spElector) Role() Role {
	return e.role
}

// Abdicate the leadership, which is useless in fact
func (e *spElector) Abdicate() {
	if e.role != RoleLeader {
		return
	}

	e.updateRole(RoleLeader, RoleFollower)
}

// Promote as a leader, which is useless in fact
func (e *spElector) Promote() {
	if e.role == RoleLeader {
		return
	}

	e.updateRole(e.role, RoleLeader)
}

func (e *spElector) updateRole(from, to Role) {
	if e.role == to || e.role != from {
		return
	}

	e.role = to

	saveState(e.stFile, e.role, 0)
}
