#!/usr/bin/env python3


""" Send messages to Graylog using a TCP port.
"""


from .graylog_client import Graylog_Client


class Graylog_Client_TCP(Graylog_Client):
    """ Send messages to Graylog using a TCP port.
    """


    import socket


    #: Tuple representing Graylog host and port.
    #: Useful in case we need to reconnect.
    _destination = (None, None)
    #: Socket used to connect Graylog.
    _sock = None


    def __init__(self, host, port, timeout):
        """ Establish a connection to Graylog. """
        self._sock = self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(timeout)

    def __del__(self):
        """ Close connections to Graylog. """
        self._sock.close()

    def send(self, gelf_message):
        """ Send the specified TCP packet. """
        # @TODO
        # The final NUL character is required by Graylog, according to the docs.
        # But it won't work when testing with netcat.
        # Test against a Graylog server.
        #self._sock.sendall(gelf_message) + '\0'
        self._sock.sendall(gelf_message)
        self._sock.recv(1024)

#EOF