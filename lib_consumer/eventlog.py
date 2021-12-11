#!/usr/bin/env python


""" Includes the eventlog handler. """


class Eventlog:
    """ Eventlog handler. """

    # Log
    #
    # The log contains rows in this format:
    #
    # READ:12345:slow.log
    # SENT:12345:slow.log
    #
    # The first column tells us whether the entry refers to read rows
    # (from the original source) or to rows sent to their destination.
    # Rotation is supposed to happen via logrotate.
    # The module we use will automatically close and reopen the file
    # if logrotate truncates it.

    #: Path of the eventlog file
    _EVENTLOG_PATH = '/var/mariadb-to-graylog/logs'
    # Name of the eventlog file
    _EVENTLOG_NAME = 'events.log'

    #: Separator between fields, in the same line
    FIELD_SEPARATOR = ':'

    #: Eventlog file handler
    _handler = None


    ##  Methods
    ##  =======

    def _get_name(self):
        """ Compose the file for a new eventlog """
        return self._EVENTLOG_PATH + '/' + self._EVENTLOG_NAME

    def __init__(self):
        """ Open newest log file. If the file is changed (eg by logrotate) it closes and reopens it. """
        file = self._get_name()
        try:
            self._handler = open(file, 'a')
        except:
            abort(3, 'Could not open or create eventlog: ' + eventlog_file)

    def append(self, action, position, sourcefile):
        """ Append a line to the Eventlog """
        self._handler.write(action + self.FIELD_SEPARATOR + position + self.FIELD_SEPARATOR + sourcefile + "\n")

    def close(self):
        """ Close the Eventlog """
        self._handler.close()

#EOF