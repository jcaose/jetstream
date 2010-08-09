
from collections import deque
import tornado.ioloop
import tornado
import errno
import logging
import socket


class IOStream(object):
    def __init__(self, socket, io_loop=None, max_buffer_size=104857600,
                 read_chunk_size=4096):
        self.socket = socket
        self.socket.setblocking(False)
        self.io_loop = io_loop or ioloop.IOLoop.instance()
        self.max_buffer_size = max_buffer_size
        self.read_chunk_size = read_chunk_size
        self._read_buffer = deque()
        self._read_buffer_size = 0
        self._write_buffer = deque()
        self._write_buffer_size = 0
        self._read_bytes = None
        self._read_callback = None
        self._close_callback = None
        self._state = self.io_loop.ERROR
        self.io_loop.add_handler(
            self.socket.fileno(), self._handle_events, self._state)


    def read_bytes(self, num_bytes, callback):
        """Call callback when we read the given number of bytes."""
        assert not self._read_callback, "Already reading"
        if self._read_buffer_size >= num_bytes:
            callback(self._consume(num_bytes))
            return
        self._check_closed()
        self._read_bytes = num_bytes
        self._read_callback = callback
        self._add_io_state(self.io_loop.READ)

    def write(self, data, callback=None):
        """Write the given data to this stream.
        """
        self._check_closed()
        self._write_buffer.append( (data, callback))
        self._write_buffer_size += len(data)
        self._add_io_state(self.io_loop.WRITE)


    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = callback

    def close(self):
        """Close this stream."""
        if self.socket is not None:
            self.io_loop.remove_handler(self.socket.fileno())
            self.socket.close()
            self.socket = None
            if self._close_callback:
                self._run_callback(self._close_callback)

    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return self._write_buffer_size > 0

    def closed(self):
        return self.socket is None

    def _handle_events(self, fd, events):
        if not self.socket:
            logging.warning("Got events for closed stream %d", fd)
            return
        if events & self.io_loop.READ:
            self._handle_read()
        if not self.socket:
            return
        if events & self.io_loop.WRITE:
            self._handle_write()
        if not self.socket:
            return
        if events & self.io_loop.ERROR:
            self.close()
            return
        state = self.io_loop.ERROR
        if self._read_bytes:
            state |= self.io_loop.READ
        if self._write_buffer_size:
            state |= self.io_loop.WRITE
        if state != self._state:
            self._state = state
            self.io_loop.update_handler(self.socket.fileno(), self._state)

    def _run_callback(self, callback, *args, **kwargs):
        try:
            callback(*args, **kwargs)
        except:
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()
            # Re-raise the exception so that IOLoop.handle_callback_exception
            # can see it and log the error
            raise

    def _handle_read(self):
        if self._read_buffer_size >= self.max_buffer_size:
            #do not recv, the writer on the other side will slow down
            #until we have consumed enough bytes
            return

        try:
            chunk = self.socket.recv(self.read_chunk_size)
        except socket.error, e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            else:
                logging.warning("Read error on %d: %s",
                                self.socket.fileno(), e)
                self.close()
                return


        if not chunk:
            self.close()
            return
        self._read_buffer.append(chunk)
        self._read_buffer_size += len(chunk)

        if self._read_buffer_size >= self.max_buffer_size:
            logging.error("read buffer overflow, close down")
            self.close()
            return

        if self._read_buffer_size >= self._read_bytes:
            num_bytes = self._read_bytes
            callback = self._read_callback
            self._read_callback = None
            self._read_bytes = None
            self._run_callback(callback, self._consume(num_bytes))


    def _handle_write(self):
        write_complete = False
        if self._write_buffer_size:

            try:
                x, callback = self._write_buffer.popleft()
                num_bytes = self.socket.send(x)
                if num_bytes < len(x):
                    self._write_buffer.appendleft( (x[num_bytes:], callback) )
                else:
                    write_complete = True
                self._write_buffer_size -= num_bytes
            except socket.error, e:
                if not (e[0] in (errno.EWOULDBLOCK, errno.EAGAIN)):
                    logging.warning("Write error on %d: %s",
                                    self.socket.fileno(), e)
                    self.close()
                    return

        if self._write_buffer_size > 0:
            self._add_io_state(self.io_loop.WRITE)
        else:
            self._remove_io_state(self.io_loop.WRITE)


        if write_complete and callback:
            self._run_callback(callback)

    def _consume(self, loc):
        x = self._read_buffer.popleft()
        length = len(x)
        if length > loc:
            result = x[:loc]
            self._read_buffer.appendleft(x[loc:])
        elif length == loc:
            result = x
        elif length < loc:
            n = len(x)
            acc = [x]
            n0 = 0
            while n < loc:
                n0 += len(x)
                x = self._read_buffer.popleft()
                acc.append(x)
                n += len(x)
            if n > loc:
                x = acc.pop()
                i = len(x) - (n-loc)
                acc.append(x[:i])
                self._read_buffer.appendleft(x[i:])
            result = ''.join(acc)

        self._read_buffer_size -= len(result)

        return result

    def _check_closed(self):
        if not self.socket:
            raise IOError("Stream is closed")

    def _add_io_state(self, state):
        if not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.socket.fileno(), self._state)


    def _remove_io_state(self, state):
        if not self._state & state:
            self._state = self._state &  (~state)
            self.io_loop.update_handler(self.socket.fileno(), self._state)

