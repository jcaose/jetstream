import socket, functools, logging, fcntl, struct, time, types, re, random
from collections import defaultdict, deque
import tornado
import tornado.ioloop
from iostream import IOStream


class Exchange(object):
    ''' Base exchange class.
        Implementing random routing for unicast message in case of multiple
        matching.
    '''

    def __init__(self):
        self._clients = {}  # client -> qid list
        self._subscribers = defaultdict(set) # qid -> client set


    def connect(self, client):
        self._clients[client] = []


    def disconnect(self, client):
        if client in self._clients:
            for qid in self._clients[client]:
                self.unsubscribe(qid, client)
            del self._clients[client]


    def subscribe(self, qid, client):
        ''' qid can be a string
            or a regular expression like object has 'match' method
        '''
        assert client in self._clients
        self._subscribers[qid].add(client)
        self._clients[client].append(qid)


    def unsubscribe(self, qid, client):
        assert client in self._clients
        self._subscribers[qid].remove(client)
        self._clients[client].remove(qid)


    def dispatch(self, qid, message, multicast):
        clients = set()

        for t in self._subscribers:
            if (type(t) == types.StringType and t == qid) or \
                    (hasattr(t, 'match') and t.match(qid)):
                clients.update(self._subscribers[t])

        if multicast:
            [c.on_message(qid, message) for c in clients]
        else:
            c = random.choice([c for c in clients])
            c.on_message(qid, message)


class Adapter(object):
    ''' Base Adapter class.
    '''

    def __init__(self, exchange):
        self._exchange = exchange

    def connect(self, client):
        self._exchange.connect(client)

    def disconnect(self, client):
        self._exchange.disconnect(client)

    def subscribe(self, qid, client):
        self._exchange.subscribe(qid, client)

    def unsubscribe(self, qid, client):
        self._exchange.unsubscribe(qid, client)

    def dispatch(self, qid, message, multicast):
        self._exchange.dispatch(qid, message, multicast)


class Client(object):
    ''' Base client class. Messages are pushed from exchange to the Client
    '''

    def __init__(self):
        self.connected = False


    def connect(self, exchange):
        assert not self.connected
        self._exchange = exchange
        exchange.connect(self)
        self.connected = True
        self.on_connected()


    def disconnect(self):
        assert self._exchange
        self._exchange.disconnect(self)
        del self._exchange
        self.connected = False
        self.on_disconnected()


    def subscribe(self, qid):
        if self.connected:
            self._exchange.subscribe(qid, self)


    def unsubscribe(self, qid):
        if self.connected:
            self._exchange.unsubscribe(qid, self)


    def send(self, qid, message, multicast=True):
        if self.connected:
            self._exchange.dispatch(qid, message, multicast)


    def on_connected(self):
        pass


    def on_disconnected(self):
        pass


    def on_message(self, qid, message):
        pass





class SocketAdapter(Adapter):
    ''' Unix Socket Server Adapter for Exchange
    '''

    def __init__(self, exchange, ioloop=None):
        Adapter.__init__(self, exchange)
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._socket = None
        self._started = False


    def _bind(self, address):
        raise Exception("not implemented")


    def start(self, address):
        self._bind(address)
        assert not self._started
        self._started = True
        self._ioloop.add_handler(self._socket.fileno(),
                self._handle_events,
                tornado.ioloop.IOLoop.READ)


    def stop(self):
        assert self._started
        self._started = False
        self._ioloop.remove_handler(self._socket.fileno())
        self._socket.close()

        for client in  self._clients:
            client.disconnect()


    def _handle_events(self, fd, events):
        try:
            socket, address = self._socket.accept()
        except socket.error as e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            raise
        try:
            stream = IOStream(socket, io_loop=self._ioloop)
            Connection(self, stream, address)
        except:
            logging.error("Error happened when creating a connection",
                    exc_info=True)



class TcpAdapter(SocketAdapter):
    ''' a TCP Adapter for the Exchange
    '''

    def __init__(self, exchange, ioloop=None):
        SocketAdapter.__init__(self, exchange, ioloop)


    def _bind(self, address):
        assert not self._socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        flags = fcntl.fcntl(self._socket.fileno(), fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(self._socket.fileno(), fcntl.F_SETFD, flags)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(0)
        host, port = address
        self._socket.bind((host, port))
        self._socket.listen(128)



class IpcAdapter(SocketAdapter):

    def __init__(self, exchange, ioloop=None):
        SocketAdapter.__init__(self, exchange, ioloop)


    def _bind(self, address):
        assert not self._socket
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        flags = fcntl.fcntl(self._socket.fileno(), fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(self._socket.fileno(), fcntl.F_SETFD, flags)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(0)
        self._socket.bind(address)
        self._socket.listen(128)


OP_CONNECT = 0
OP_CONNECTED = 1
OP_DISCONNECT = 2
OP_SUBSCRIBE = 3
OP_UNSUBSCRIBE = 4
OP_MESSAGE = 5
OP_SEND = 6


class Connection(Client):
    ''' Connection handler for the TCP and IPC client.
        It acts like "Client" on behalf of the connected TCP/IPC Client with the
        exchange server associated with the Adapter
    '''

    def __init__(self, exchange, stream, address):
        Client.__init__(self)
        self._exchange = exchange
        self._stream = stream
        self._address = address
        self._is_connected = False
        self._stream.read_bytes(4, self._on_header)
        self._stream.set_close_callback(self._on_close)
        self._mq = defaultdict(deque) #one queue for each qid
        self._fq = deque()  #fair queue of mq
        self._recving = False


    def _on_close(self):
        self.disconnect()


    def on_message(self, qid, message):
        ''' receive message from exchange and send it down to the client
        '''
        assert len(message) <= self._stream.max_buffer_size, \
            "message is too large (%d) for iostream to handle (%d)"  % \
            (len(message), self._stream.max_buffer_size)
        q = self._mq[qid]
        q.append(message)
        if len(q) == 1:
            self._fq.append((qid,q))

        if not self._recving:
            self._stream.io_loop.add_callback(self._recv)


    def _recv(self):
        try:
            qid, q = self._fq.pop()
        except IndexError:
            assert False, "fq is empty and this should not happen"
        self._recving = True
        def on_send_complete():
            if len(q) > 0:
                self._fq.append( (qid, q))
            else:
                del self._mq[qid]
            if len(self._fq) > 0:
                self._stream.io_loop.add_callback(self._recv)
            else:
                self._recving = False
        x = q.pop()
        header = (OP_MESSAGE << 29) | (len(qid) << 20) | len(x)
        try:
            self._stream.write(struct.pack('!I', header))
            self._stream.write(qid)
            self._stream.write(x, on_send_complete)
        except IOError:
            self._stream.close()


    def _on_header(self, header):
        header,  = struct.unpack('!I', header)
        op = header >> 29
        try:
            if op == OP_CONNECT:
                self._stream.write(struct.pack('!I', (OP_CONNECTED << 29)))
                self.connect(self._exchange)
                self._stream.read_bytes(4, self._on_header)
            elif op == OP_DISCONNECT:
                self._stream.close()
            elif op == OP_SUBSCRIBE:
                x = (header & 0x10000000) >> 28
                qid_length = (header & 0x0FF00000) >> 20
                message_length = header & 0x000FFFFF
                assert message_length == 0, "SUBSCRIBE frame contains non zero payload"
                def subscribe(qid):
                    qid = re.compile(qid) if x else qid
                    self.subscribe(qid)
                    self._stream.read_bytes(4, self._on_header)
                self._stream.read_bytes(qid_length, subscribe)
            elif op == OP_UNSUBSCRIBE:
                x = (header & 0x10000000) >> 28
                qid_length = (header & 0x0FF00000) >> 20
                message_length = header & 0x000FFFFF
                assert message_length==0, "UNSUBSCRIBE frame contains non zero payload"
                def unsubscribe(qid):
                    qid = re.compile(qid) if x else qid
                    self.unsubscribe(qid)
                    self._stream.read_bytes(4, self._on_header)
                self._stream.read_bytes(qid_length, unsubscribe)

            elif op == OP_SEND:
                multicast = (header & 0x10000000) >> 28
                qid_length = (header & 0x0FF00000) >> 20
                message_length = header & 0x000FFFFF

                def _send(qid, message):
                    self.send(qid, message, multicast)

                    def on_header():
                        try:
                            self._stream.read_bytes(4, self._on_header)
                        except IOError:
                            self._stream.close()
                    #need to wrap it in callback otherwise risk of infinite recursion
                    self._stream.io_loop.add_callback(functools.partial(on_header))

                def on_qid(qid):
                    if message_length > 0 :
                        try:
                            self._stream.read_bytes(message_length,
                                    functools.partial(_send, qid))
                        except IOError:
                            self._stream.close()
                    else:
                        self._stream.io_loop.add_callback(
                                functools.partial(_send, qid, ''))
                if qid_length>0:
                    self._stream.read_bytes(qid_length, on_qid)
                else:
                    on_qid('')
        except IOError:
            self._stream.close()



class SocketClient(Client):
    ''' An implementation of Unix Socket Client
    '''

    def __init__(self, ioloop):
        Client.__init__(self)
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._stream = None
        self.connected = False
        self._sending = deque()


    def add_timeout(self, t, f):
        return self._ioloop.add_timeout(t, f)


    def remove_timeout(self, timeout):
        self._ioloop.remove_timeout(timeout)


    def add_callback(self, f):
        self._ioloop.add_callback(f)

    def remove_callback(self, f):
        self._ioloop.remove_callback(f)

    def _on_connected(self):
        self.connected = True
        self.on_connected()


    def _on_disconnected(self):
        self.connected = False
        self.on_disconnected()


    def close(self):
        self._stream.write(struct.pack('!I', OP_DISCONNECT << 29),
                self._stream.close)


    def connect(self, address):
        raise Exception("Not implemented")


    def subscribe(self, qid):
        x = 1 if type(qid) != types.StringType and hasattr(qid, 'pattern') else 0
        qid = qid.pattern if x else qid
        qid_length = len(qid)
        assert qid_length <= 255
        header = (OP_SUBSCRIBE << 29) | x << 28 | (qid_length << 20)
        self._stream.write(struct.pack('!I', header))
        self._stream.write(qid)


    def unsubscribe(self, qid):
        x = 1 if type(qid) != types.StringType and hasattr(qid, 'pattern') else 0
        qid = qid.pattern if x else qid
        assert len(qid)<=255
        header = (OP_UNSUBSCRIBE << 29) | x << 28 | (qid_length << 20)
        self._stream.write(struct.pack('!I', header))
        self._stream.write(qid)


    def send(self, qid, message, multicast=True):
        if self._stream.closed():
            raise IOError("Connection to exchange is closed")


        self._sending.append((qid, message, multicast))
        if len(self._sending) == 1:
            self._send()


    def _send(self):
        qid, message, multicast = self._sending.popleft()
        qid_length = len(qid)
        assert len(qid) <= 255
        message_length = len(message)
        assert len(message) <= self._stream.max_buffer_size
        header = (OP_SEND << 29) | (1 if multicast else 0) << 28 \
                | (qid_length << 20) | message_length
        try:
            self._stream.write(struct.pack('!I', header))
            self._stream.write(qid)
            self._stream.write(message)
        except IOError:
            self._stream.close()

        if len(self._sending) > 0 and not self._stream.closed():
            self.add_callback(self._send)


    def _on_header(self, header):
        header, = struct.unpack('!I', header)
        op = header >> 29
        try:
            if op == OP_MESSAGE:
                qid_length = (header & 0x0FF00000) >> 20
                message_length = header & 0x000FFFFF
                def recv(qid, message):
                    self.on_message(qid, message)
                    self._stream.read_bytes(4, self._on_header)

                def on_qid(qid):
                    if message_length > 0 :
                        self._stream.read_bytes(message_length,
                                functools.partial(recv, qid))
                    else:
                        recv(qid,  '')
                if qid_length > 0:
                    self._stream.read_bytes(qid_length, on_qid)
                else:
                    on_qid('')
            else:
                assert False, "Unknown op code 0x%02X" % op
        except IOError:
            self._stream.close()



class TcpClient(SocketClient):

    def __init__(self, ioloop):
        SocketClient.__init__(self, ioloop)


    def connect(self, address):
        host, port = self._adress = address
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((host, port))

        self._stream = stream = IOStream(s, self._ioloop)
        stream.set_close_callback(self._on_disconnected)
        stream.write(struct.pack('!I', OP_CONNECT << 29))
        def on_connected(header):
            header, = struct.unpack('!I', header)
            if (header >> 29) != OP_CONNECTED:
                raise Exception("handshake error with" + str(address))

            self._on_connected()
            self._stream.read_bytes(4, self._on_header)

        stream.read_bytes(4, on_connected)



class IpcClient(SocketClient):

    def __init__(self, ioloop):
        SocketClient.__init__(self, ioloop)


    def connect(self, address):
        self._adress = address
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        s.connect(address)

        self._stream = stream = IOStream(s, self._ioloop)
        stream.set_close_callback(self._on_disconnected)
        stream.write(struct.pack('!I', OP_CONNECT << 29))
        def on_connected(header):
            header, = struct.unpack('!I', header)
            if (header >> 29) != OP_CONNECTED:
                raise Exception("handshake error with" + str(address))

            self._on_connected()
            self._stream.read_bytes(4, self._on_header)

        stream.read_bytes(4, on_connected)

