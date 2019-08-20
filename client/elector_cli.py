import socket
import subprocess
import sys
from enum import Enum

"""
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
"""

reqRole = '?00;'
reqAbdicate = '?10;'
reqPromote = '?20;'

rplRoleCandidate = '0'
rplRoleFollower = '1'
rplRoleLeader = '2'
rplRoleUnstable = '5'


class BadReplyException(Exception):
    pass


class Role(Enum):
    Candidate = 'candidate'
    Follower = 'follower'
    Leader = 'leader'
    Unstable = 'unstable'


class Client(object):
    def __init__(self, host, path='/tmp/dms.elector.sock', timeout=30):
        self._ip, self._port = host.split(':')
        self._port = int(self._port)
        self._path = path
        self._timeout = timeout

        self._sock = None

        self._local = False
        for addr in subprocess.check_output(['hostname', '--all-ip-addresses']).decode().split(' '):
            if addr == self._ip:
                self._local = True
                break

    def connect(self):
        try:
            self._sock = socket.socket(socket.AF_UNIX if self._local else socket.AF_INET)
            self._sock.connect(self._path if self._local else (self._ip, self._port))
        except Exception as e:
            raise e

    def close(self):
        self._sock.close()

    def role(self) -> (Role, bool):
        try:
            self._sock.sendall(reqRole.encode())
            data = self._sock.recv(4).decode()

            if data[0] != '!' or data[3] != ';':
                raise BadReplyException()

            if data[1] != '1':
                return None, False

            if data[2] == rplRoleCandidate:
                return Role.Candidate, True
            if data[2] == rplRoleFollower:
                return Role.Follower, True
            if data[2] == rplRoleLeader:
                return Role.Leader, True
            if data[2] == rplRoleUnstable:
                return Role.Unstable, True
        except Exception as e:
            raise e

    def abdicate(self) -> bool:
        try:
            self._sock.sendall(reqAbdicate.encode())
            data = self._sock.recv(4).decode()

            if data[0] != '!' or data[3] != ';':
                raise BadReplyException()

            if data[1] != '1':
                return False

            return True
        except Exception as e:
            raise e

    def promote(self) -> bool:
        try:
            self._sock.sendall(reqPromote.encode())
            data = self._sock.recv(4).decode()

            if data[0] != '!' or data[3] != ';':
                raise BadReplyException()

            if data[1] != '1':
                return False

            return True
        except Exception as e:
            raise e


VERSION = 'alpha.unix-domain.1'
DATE = '20180903'


def main():
    if len(sys.argv) not in (2, 3):
        print('wrong args, usage: python3 elector_cli.py ip:port role|abdicate|promote')
        sys.exit(-1)

    if len(sys.argv) == 2 and sys.argv[1] == '--version':
        print('elector client version', VERSION, DATE)
        return

    client = Client(sys.argv[1])
    try:
        client.connect()

        if sys.argv[2] == 'role':
            r, ok = client.role()
            if not ok:
                print('request denied')
            else:
                print('role:', r.value)
        elif sys.argv[2] == 'abdicate':
            if client.abdicate():
                print('request received')
            else:
                print('request denied')
        elif sys.argv[2] == 'promote':
            if client.promote():
                print('request received')
            else:
                print('request denied')
        else:
            print('wrong args, usage: elector_cli ip:port role|abdicate|promote')

        client.close()
    except Exception as e:
        print("request failed:", e)


if __name__ == '__main__':
    main()
