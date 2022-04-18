#!/usr/bin/env python3


""" Send messages to Graylog.
    Children class will choose a protocol.
"""


from .gelf_message import GELF_Message


class Graylog_Client:
    """ Send messages to Graylog.
    """


    def __init__(self, host: str, port: str):
        """ Assign values to private members. """
        pass

    def send(self, gelf_message: GELF_Message) -> None:
        """ Send the specified UDP packet. """
        pass

#EOF