#!/usr/bin/env python


""" Includes the eventlog handler. """


import os


class Eventlog:
    """
        Eventlog handler.

        The log contains rows in this format:

        READ:12345:slow.log
        SENT:12345:slow.log

        The first column tells us whether the entry refers to read rows
        (from the original source) or to rows sent to their destination.
        Rotation is supposed to happen via logrotate.
        The module we use will automatically close and reopen the file
        if logrotate truncates it.
    """

    ##  Constants
    ##  =========

    #: Path of the eventlog file
    _EVENTLOG_PATH = '/var/mariadb-to-graylog/logs'
    # Name of the eventlog file
    _EVENTLOG_NAME = 'events.log'

    #: Separator between fields, in the same line
    FIELD_SEPARATOR = ':'

    #: Eventlog file handler
    _handler = None
    #: Initial offset
    _offset = None


    ##  Methods
    ##  =======

    def _get_name(self):
        """ Compose the file for a new eventlog """
        return self._EVENTLOG_PATH + '/' + self._EVENTLOG_NAME

    def __init__(self, options):
        """ Open newest log file. If the file is changed (eg by logrotate) it closes and reopens it. """
        file = self._get_name()

        # If the Eventlog exists and we're not going to truncate it,
        # read the offset from the last line and store it in self._offset,
        # so it can be read by the program.
        if os.path.exists(file) and not options['truncate']:
            self._handler = open(file, 'r')
            last_line = self._handler.readline()
            while last_line:
                prev_line = last_line
                last_line = self._handler.readline()
            self._handler.close()
            self._offset = int(prev_line.split(':')[0])

        # Empty the file if required
        if options['truncate']:
            try:
                self._handler = open(file, 'w')
                self._handler.truncate(0)
                self._handler.close()
                self._handler = open(file, 'a')
            except:
                raise Exception('Could not open or create eventlog: ' + file)
        else:
            try:
                self._handler = open(file, 'a')
            except:
                raise Exception('Could not open or create eventlog: ' + file)

    def get_offset(self):
        """ Return the Eventlog offset from the previous run """
        return self._offset

    def append(self, position, sourcefile):
        """ Append a line to the Eventlog """
        self._handler.write(position + self.FIELD_SEPARATOR + sourcefile + "\n")

    def close(self):
        """ Close the Eventlog """
        self._handler.close()

#EOF