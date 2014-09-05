import socket
import json
import sys

def send(host, port, message, group='cosmos', title='cosmos', subtitle=''):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect((host, port))
        try:
            d = json.dumps(dict(group=group, title=title, subtitle=subtitle, message=message))
            sock.sendall(d)
            assert sock.recv(1024) == 'Received', 'unexpected response'
        finally:
            sock.close()
    except socket.error as e:
        print >> sys.stderr, 'ERROR sending message: %s'%e


if __name__ == '__main__':
    import argparse

    p = argparse.ArgumentParser()

    p.add_argument('-H','--host', default='localhost')
    p.add_argument('-p','--port', type=int, default=4947)
    p.add_argument('message', nargs='+')
    args = p.parse_args()
    send(args.host, args.port, ' '.join(args.message))