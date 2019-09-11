package server

import (
	"math/rand"

	"github.com/sirupsen/logrus"
)

/********** single point elector **********/

// spElector is the elector when running in single point mode,
// it has nothing meaningful in fact cuz no need to build a leader-follower relationship,
// but used as well...

// NOTE: id here seems useless
type spElector struct {
	id    uint64
	role  Role
	epoch uint64

	stFile string // state file with role and epoch

	rs *roleService // gRPC service
}

// NewSinglePoint is the constructor of spElector
func NewSinglePoint(stfile, rsTcpHost, rsUnixHost string) *spElector {
	role, _ := loadState(stfile)
	return newSinglePointWithInfo(stfile, role, rsTcpHost, rsUnixHost)
}

func newSinglePointWithInfo(stfile string, role Role, rsTcpHost, rsUnixHost string) *spElector {
	e := new(spElector)

	e.id = rand.Uint64()
	e.role = role
	e.epoch = 0
	e.stFile = stfile

	e.rs = newRoleService(rsTcpHost, rsUnixHost, e)

	return e
}

// Info gets metadata of the elector
func (e *spElector) Info() ElectorInfo {
	return ElectorInfo{e.id, e.role, e.epoch}
}

// Start launch a single-point elector
func (e *spElector) Start() error {
	// NOTE: always start as a leader in single-point mode
	e.updateRole(e.role, RoleLeader)

	if err := e.rs.Start(); err != nil {
		logrus.Warnf("[single-point] start grpc-role-service failed, reason: %v", err)
		return err
	}

	return nil
}

// Stop stops the single-point elector
func (e *spElector) Stop() error {
	if err := saveState(e.stFile, e.role, 0); err != nil {
		return err
	}

	if err := e.rs.Stop(); err != nil {
		return err
	}

	return nil
}

// Role gets the role of the elector
func (e *spElector) Role() Role {
	return e.role
}

// Abdicate the leadership
func (e *spElector) Abdicate() {
	if e.role != RoleLeader {
		return
	}

	e.updateRole(RoleLeader, RoleFollower)
}

// Promote as a leader
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
