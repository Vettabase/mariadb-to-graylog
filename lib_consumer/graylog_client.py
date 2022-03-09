#!/usr/bin/env python3


""" Send messages to Graylog.
    Children class will choose a protocol.
"""


class Graylog_Client:
    """ Send messages to Graylog.
    """


    def __init__(self, host, port):
        """ Assign values to private members. """
        pass

    def send(self, gelf_message):
        """ Send the specified UDP packet. """
        pass

#EOF