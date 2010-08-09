import jetstream
import logging
import time, functools, re
import tornado

logging.basicConfig()

MSG = 'x'*1000*1000
BUKCET_RATE = 100

class Client(jetstream.TcpClient):
    def __init__(self, ioloop):
        jetstream.TcpClient.__init__(self, ioloop)
        self._transmitting = False

    def refill_bucket(self):
        self._bucket = BUKCET_RATE
        self._bucket_refill_timeout = self.add_timeout(time.time() + 1.0, self.refill_bucket)
        if not self._transmitting:
            self.add_callback(self.transmit)


    def transmit(self):
        if self._bucket > 0 and self.connected:
            self.send('/queue', MSG, multicast=False)
            self._bucket -= 1
            if self._bucket > 0 :
                self.add_callback(self.transmit)
                self._transmitting = True
            else:
                self._transmitting = False


    def on_connected(self):
        print "connected to server"
        self.subscribe('/')
        ## on can also subscribe using regular expression
        #self.subscribe(re.compile(r'.client.b'))
        self.send('/', 'start')
        ## stop after 5 seconds
        self.add_timeout(time.time()+5.0,
                functools.partial(self.send, '/', 'stop'))

        self._bucket_refill_timeout = self.add_timeout(time.time() + 1.0, self.refill_bucket)


    def on_disconnected(self):
        print "disconnected from server"


    def on_message(self, qid, message):
        if qid =='/':
            if message=='start':
                self.refill_bucket()
            elif message=='stop':
                self.remove_timeout(self._bucket_refill_timeout)
                self.close()


def main():
    ioloop = tornado.ioloop.IOLoop()
    client = Client(ioloop)
    ioloop.add_callback(functools.partial(client.connect, ("127.0.0.1", 8000)))
    ioloop.start()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass



