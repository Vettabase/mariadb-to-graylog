#!/usr/bin/env python


""" Send messages to Graylog using a UDP port.
"""


class Graylog_Client_UDP:
    """ Send messages to Graylog using a UDP port.
    """

    #: Graylog host.
    _host = None
    #: Graylog UDP port to use.
    _port = None


    def __init__(self, host, port):
        """ Assign values to private members. """
        self._host = host
        self._port = port

    def send(self, gelf_message):
        """ Send the specified UDP packet. """
        self.socket.socket(self.socket.AF_INET, self.socket.SOCK_DGRAM).sendto(
            # python3: bytes(gelf_message, 'utf-8'),
            gelf_message,
            (self._host, self._port)
        )

#EOF