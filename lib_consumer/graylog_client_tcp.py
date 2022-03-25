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


    def __init__(self, host, port):
        """ Establish a connection to Graylog. """
        _sock = self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM)
        _sock.connect((host, port))

    def __del__(self):
        """ Close connections to Graylog. """
        _sock.close()

    def send(self, gelf_message):
        """ Send the specified TCP packet. """
        _sock.sendall(gelf_message)
        _sock.recv(1024)

#EOF