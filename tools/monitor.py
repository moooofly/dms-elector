import argparse
import socket
import sys
import time
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
    def __init__(self, host, timeout=30):
        ip, port = host.split(':')
        self._host = (ip, int(port))
        self._timeout = timeout

        self._conn = None

    def connect(self):
        try:
            self._conn = socket.create_connection(self._host, self._timeout)
        except Exception as e:
            raise e

    def close(self):
        self._conn.close()

    def role(self) -> (Role, bool):
        try:
            self._conn.sendall(reqRole.encode())
            data = self._conn.recv(4).decode()

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


def monitor(hosts, interval):
    clients = [Client(h) for h in hosts]
    for c in clients:
        c.connect()

    try:
        print('====================================================')
        while True:
            print(time.asctime(time.localtime(time.time())))
            print()
            for c in clients:
                print(c._host[0] + ':' + str(c._host[1]), c.role()[0])
            print('====================================================')

            time.sleep(interval)
    except KeyboardInterrupt:
        for c in clients:
            c.close()
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('hosts', nargs='+')
    parser.add_argument('--i', type=float, default=1.0)
    args = parser.parse_args(sys.argv[1:])
    monitor(args.hosts, args.i)


if __name__ == '__main__':
    main()
