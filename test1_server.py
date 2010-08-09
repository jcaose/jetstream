import jetstream
import time
import re
import tornado
import logging

logging.basicConfig()



class Client(jetstream.Client):
    def __init__(self, exchange, name):
        jetstream.Client.__init__(self)
        self._name = name
        self.connect(exchange)


    def on_connected(self):
        print self._name, " connected"
        self.subscribe('/')
        self.subscribe('/queue')

    def on_disconnected(self):
        print self._name, " disconnected"


    def on_message(self, qid, message):
        if qid == '/queue':
            t, n, s = self._tic
            self._tic = t, n+1, s+len(message)
        elif qid =='/':
            print self._name, qid, message, time.time()
            if message == 'start':
                t = time.time()
                print self._name, "start ticking", t
                self._tic = (t, 0, 0)

            elif message == 'stop':
                t1 = time.time()
                print self._name, "stop ticking", t1
                t0, n, s = self._tic
                t = t1 -t0
                print self._name, "throughtput ", n*1.0/t/1e3,'Kmessages/s' ,s*1.0/t/1024./1024.,'MB/S'


def main():
    ioloop = tornado.ioloop.IOLoop()
    exchange = jetstream.Exchange()

    client_a1 = Client(exchange, "A1")
    client_a2 = Client(exchange, "A2")


    tcp_adapter = jetstream.TcpAdapter(exchange, ioloop)
    tcp_adapter.start(("127.0.0.1", 8000))
    ioloop.start()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
