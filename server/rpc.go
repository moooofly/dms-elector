package server

import (
	"io"
	"net"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

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
	tcpHost  string
	unixHost string

	e Elector

	lnrs []net.Listener

	stopCh chan struct{}
}

func newRequestServer(host, sock string, e Elector) *requestServer {
	return &requestServer{tcpHost: host, unixHost: sock, e: e}
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

// TODO: 这里的写法可以优化，这里需要 tcp 和 unix sock 同时监听么
func (rs *requestServer) start() error {
	var tcpLsnr, unixLsnr net.Listener
	var err error

	rs.stopCh = make(chan struct{})

	if rs.tcpHost != "" {
		tcpLsnr, err = net.Listen("tcp", rs.tcpHost)
		if err != nil {
			return err
		}
		rs.lnrs = append(rs.lnrs, tcpLsnr)
	}
	if rs.unixHost != "" {
		// remove socket file if exists
		if _, err := os.Stat(rs.unixHost); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else if err = os.Remove(rs.unixHost); err != nil {
			return err
		}
		unixLsnr, err = net.Listen("unix", rs.unixHost)
		if err != nil {
			return err
		}
		rs.lnrs = append(rs.lnrs, unixLsnr)
	}

	// NOTE: both tcp and unix sock being used
	for _, l := range rs.lnrs {
		go func(l net.Listener) {
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
