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
    # Whether TCP messages should end with a NUL character.
    # This is necessary with Graylog, but breaks netcat.
    _terminate_with_nul = True
    # Expected length of Graylog answer
    _answer_length = 1024


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
        if self._terminate_with_nul:
            self._sock.sendall(gelf_message + '\0')
        self._sock.sendall(gelf_message)
        self._sock.recv(self._answer_length)

#EOF