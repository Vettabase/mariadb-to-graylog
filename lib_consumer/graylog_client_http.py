#!/usr/bin/env python


""" Send messages to Graylog using a TCP port.
"""


from graylog_client import Graylog_Client


class Graylog_Client_HTTP(Graylog_Client):
    """ Send messages to Graylog using a TCP port.
    """


    import requests
    # Needed to convert JSON string to dictionary
    import json


    #: Graylog URL that will receive requests, including host and port.
    _url = None
    #: Socket used to connect Graylog.
    _sock = None


    def __init__(self, host, port=12201):
        """ Compose Graylog URL. """
        self._url = 'http://' + host + ':' + str(port) + '/gelf'

    def send(self, gelf_message):
        """ Send the specified GELF message over an HTTP request. """
        self.requests.get(self._url, data=self.json.loads(gelf_message))

#EOF