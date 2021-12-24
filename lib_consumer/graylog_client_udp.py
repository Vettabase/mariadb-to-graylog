#!/usr/bin/env python


""" Send messages to Graylog using a UDP port.
"""


from graylog_client import Graylog_Client


class Graylog_Client_UDP(Graylog_Client):
    """ Send messages to Graylog using a UDP port.
    """


    import socket


    #: An immutable tuple (host, port) is assigned when the
    #: object is instantiated.
    _destination = (None, None)


    def __init__(self, host, port):
        """ Assign values to private members. """
        self._destination = (host, port)

    def send(self, gelf_message):
        """ Send the specified UDP packet. """
        self.socket.socket(self.socket.AF_INET, self.socket.SOCK_DGRAM).sendto(
            # python3: bytes(gelf_message, 'utf-8'),
            gelf_message,
            self._destination
        )

#EOF