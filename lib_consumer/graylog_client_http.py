#!/usr/bin/env python3


""" Send messages to Graylog using a TCP port.
"""


from .graylog_client import Graylog_Client


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
            success = False
            attempts_left = 3
            while (not success) and attempts_left > 0:
                attempts_left = attempts_left - 1
                try:
                    self.requests.post(
                        self._url,
                        headers={
                            'Content-Type': 'application/json',
                            'User-Agent': 'Vettabase/mariadb-to-graylog'
                        },
                        json=self.json.loads(gelf_message),
                        timeout=self._graylog_http_timeout_idle,
                        verify=True,
                        allow_redirects=False
                    )
                    success = True
                # requests exceptions are listed here:
                # https://docs.python-requests.org/en/latest/user/quickstart/#errors-and-exceptions
                except Exception as e:
                    print(str(e))

#EOF