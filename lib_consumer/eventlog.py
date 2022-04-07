#!/usr/bin/env python3


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


    from pathlib import Path


    ##  Constants
    ##  =========

    #: Path of the eventlog file.
    #: Default is ~/logs which works for any user with a home.
    _DEFAULT_EVENTLOG_PATH = str(Path.home()) + '/logs/events.log'
    #: Temporary Eventlog extension
    _EVENTLOG_TMP_EXTENSION = '.tmp'

    #: Separator between fields, in the same line
    FIELD_SEPARATOR = ':'

    #: Eventlog file handler
    _handler = None
    #: Initial offset
    _offset = None


    ##  Methods
    ##  =======

    def _get_name_regular(self):
        """ Get the filename for a new eventlog.
            It's not guaranteed that the file doesn't exist.
        """
        return self._eventlog_path

    def _get_name_tmp(self):
        """ Get the filename for a temporary eventlog.
            It's not guaranteed that the file doesn't exist.
        """
        return self._get_name_regular() + self._EVENTLOG_TMP_EXTENSION

    def __init__(self, options, eventlog_path=None):
        """ Open newest log file. If the file is changed (eg by logrotate) it closes and reopens it. """
        # This additional check is because a class member can't be an argument default.
        if eventlog_path is None:
            eventlog_path=self._DEFAULT_EVENTLOG_PATH
        self.Path(eventlog_path).parent.resolve().mkdir(parents=True, exist_ok=True)

        # If the Eventlog exists and we're not going to truncate it,
        # read the offset from the last line and store it in self._offset,
        # so it can be read by the program.
        if os.path.exists(eventlog_path) and not options['truncate']:
            self._handler = open(eventlog_path, 'r')
            last_line = self._handler.readline()
            prev_line = None
            while last_line:
                prev_line = last_line
                last_line = self._handler.readline()
            self._handler.close()
            if prev_line is None:
                raise Exception('Eventlog is malformed')
            self._offset = int(prev_line.split(':')[0])

        # Empty the file if required
        if options['truncate']:
            try:
                self._handler = open(eventlog_path, 'w')
                self._handler.truncate(0)
                self._handler.close()
                self._handler = open(eventlog_path, 'a')
            except:
                raise Exception('Could not open or create eventlog: ' + eventlog_path)
        # Open the existing file for append
        else:
            try:
                self._handler = open(eventlog_path, 'a')
            except:
                raise Exception('Could not open or create eventlog: ' + eventlog_path)

    def get_offset(self):
        """ Return the Eventlog offset from the previous run """
        return self._offset

    def append(self, position, sourcefile):
        """ Append a line to the Eventlog """
        self._handler.write(position + self.FIELD_SEPARATOR + sourcefile + '\n')

    def close(self):
        """ Close the Eventlog """
        self._handler.close()

    def rotate(self):
        """ Rotate the Eventlog.
            If the operation fails before the creation and opening of a new file,
            the old file will remain untouched or it will be renamed but not
            deleted.
        """
        # Rotating means:
        #   - close the existing logfile
        #   - rename it
        #   - create the new logfile
        #   - open the new logfile
        #   - delete the old logfile

        file_name_regular = self._get_name_regular()
        file_name_tmp = self._get_name_tmp()

        is_closed = False
        is_renamed = False
        is_reopened = False

        try:
            self._handler.close()
            is_closed = True
            if os.path.exists(file_name_tmp):
                os.rename.unlink(file_name_tmp)
            is_purged = True
            os.rename(file_name_regular, file_name_tmp)
            is_renamed = True
            self._handler = open(file_name_regular, 'a')
            is_reopened = True
            os.unlink(file_name_tmp)
        except:
            if is_reopened:
                status = 'The new logfile was opened, but the old could not be deleted.'
            elif is_renamed:
                status = 'The old logfile was renamed, but the new file could not be created.'
            elif is_closed:
                status = 'The old logfile was closed, but it could not be renamed.'
            else:
                status = 'The old logfile could not be closed.'
            abort(3, 'An error occurred during rotation. ' + status)

#EOF