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
    # Used to implement timeouts
    import eventlet


    #: Graylog URL that will receive requests, including host and port.
    _url = None
    #: Socket used to connect Graylog.
    _sock = None
    #: HTTP requests timeout when no data is received.
    _graylog_http_timeout_idle = None
    #: HTTP requests timeout, hard limit.
    _graylog_http_timeout = None


    def __init__(self, host, port=12201, graylog_http_timeout_idle=None, graylog_http_timeout=None):
        """ Compose Graylog URL. """
        self._url = 'http://' + host + ':' + str(port) + '/gelf'

    def send(self, gelf_message):
        """ Send the specified GELF message over an HTTP request. """
        # Set a hard timeout for the HTTP call
        self.eventlet.monkey_patch()
        with self.eventlet.Timeout(self._graylog_http_timeout):
            self.requests.post(
                self._url,
                json=self.json.loads(gelf_message),
                timeout=self._graylog_http_timeout_idle,
                verify=False
            )

#EOF