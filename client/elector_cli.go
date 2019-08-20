package client

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Role represents the role of the elector
type Role uint

const (
	// RoleCandidate represents a candidate
	RoleCandidate = Role(0)

	// RoleFollower represents a follower
	RoleFollower = Role(1)

	// RoleLeader represents a leader
	RoleLeader = Role(2)
)

func (r Role) String() string {
	switch r {
	case RoleCandidate:
		return "Candidate"
	case RoleFollower:
		return "Follower"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

var usrReqRole = []byte{'?', '0', '0', ';'}
var usrReqAbdicate = []byte{'?', '1', '0', ';'}
var usrReqPromote = []byte{'?', '2', '0', ';'}

var rpl2role = map[byte]Role{'0': RoleCandidate, '1': RoleFollower, '2': RoleLeader}

// EleCli is the client of the elector
type EleCli struct {
	host string
	path string

	timeout uint

	conn net.Conn
}

// NewClient returns a elector client
func NewClient(host, path string, timeout uint) *EleCli {
	return &EleCli{host: host, path: path, timeout: timeout}
}

// Connect the elector user request server
// We prefer to use unix domain socket to decrease the TIME_WAIT hanging socket when using TCP
func (ec *EleCli) Connect() error {
	// check if the server we want to connect with is in the same machine with myself
	var local bool
	var ip string

	ip = strings.Split(ec.host, ":")[0]

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil && ipnet.IP.To4().String() == ip {
				local = true
				break
			}
		}
	}

	if local && ec.path != "" {
		c, err := net.Dial("unix", ec.path)
		if err != nil {
			return err
		} else {
			ec.conn = c
			return nil
		}
	}

	c, err := net.DialTimeout("tcp", ec.host, time.Duration(ec.timeout)*time.Second)
	if err != nil {
		return err
	}
	ec.conn = c
	return nil
}

// Close the connection with the server
func (ec *EleCli) Close() {
	ec.conn.Close()
}

// Role return the current elector's role
func (ec *EleCli) Role() (Role, error) {
	_, err := ec.conn.Write(usrReqRole)
	if err != nil {
		logrus.Debugf("cannot get role, err: %v", err)
		return RoleCandidate, err
	}

	buff := make([]byte, 4)
	_, err = ec.conn.Read(buff)
	if err != nil {
		logrus.Debugf("cannot get role, err: %v, reply: %v", err, buff)
		return RoleCandidate, err
	}

	if buff[0] != '!' || buff[3] != ';' {
		err = fmt.Errorf("incorrect reply: %v", buff)
		return RoleCandidate, err
	}

	if buff[1] == '0' {
		err = fmt.Errorf("request refused by server")
		return RoleCandidate, err
	}

	return rpl2role[buff[2]], nil
}

// Abdicate the leadership
func (ec *EleCli) Abdicate() (bool, error) {
	_, err := ec.conn.Write(usrReqAbdicate)
	if err != nil {
		logrus.Debugf("cannot send abdicate request, err: %v", err)
		return false, err
	}

	buff := make([]byte, 4)
	_, err = ec.conn.Read(buff)
	if err != nil {
		logrus.Debugf("cannot get abdicate reply, err: %v", err)
		return false, err
	}

	if buff[0] != '!' || buff[3] != ';' {
		err = fmt.Errorf("incorrect reply: %v", buff)
		return false, err
	}

	if buff[1] == '1' {
		err = fmt.Errorf("request refused by server")
		return false, err
	}

	return true, nil
}

// Promote to leader
func (ec *EleCli) Promote() (bool, error) {
	_, err := ec.conn.Write(usrReqPromote)
	if err != nil {
		logrus.Debugf("cannot send abdicate request, err: %v", err)
		return false, err
	}

	buff := make([]byte, 4)
	_, err = ec.conn.Read(buff)
	if err != nil {
		logrus.Debugf("cannot get abdicate reply, err: %v", err)
		return false, err
	}

	if buff[0] != '!' || buff[3] != ';' {
		err = fmt.Errorf("incorrect reply: %v", buff)
		return false, err
	}

	if buff[1] == '1' {
		err = fmt.Errorf("request refused by server")
		return false, err
	}

	return true, nil
}
