import threading
import time
import SocketServer
import subprocess as sp
import json


def terminal_notify(group, title, subtitle, message):
    sp.check_call("terminal-notifier -group '{group}' -title '{title}' -subtitle '{subtitle}' -sound default -message '{message}'".format(**locals()), shell=True)


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        # cur_thread = threading.current_thread()
        # response = "{}: {}".format(cur_thread.name, data)
        j = json.loads(data)
        print 'Received: %s' % j
        terminal_notify(**j)
        self.request.sendall('Received')


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


def main(host, port):
    # Port 0 means to select an arbitrary unused port

    server = ThreadedTCPServer((host, port), ThreadedTCPRequestHandler)
    # ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    try:
        server_thread.start()
        print "Server loop running on %s:%s" %(host, port)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'Shutting down'
        server.shutdown()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument('-H', '--host', default='0.0.0.0')
    p.add_argument('-p', '--port', default=4947)
    main(**vars(p.parse_args()))