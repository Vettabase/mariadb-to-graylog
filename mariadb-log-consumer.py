#!/usr/bin/env python


import sys
import signal

from lib_consumer import *
from registry import Registry


class Consumer:
    """
    Flexible metaclass for defining useful decorator functions.
    """

    ##  Modules
    ##  =======

    import time
    import os
    #import pidfile
    import datetime


    ##  Members
    ##  =======

    # The lock file is stored here.
    _LOCK_FILE_PATH = '/tmp'
    #: Identifies a run of this program.
    _label = 'default'
    #: Lock file handler.
    #: We open this file with an exclusive lock to make sure only
    #: one istance of the consumer is running for a given label.
    _lock_file = None
    # Path and name of the lock file.
    _lock_file_name = None

    #: By default this is True at the beginning of the program
    #: and means that it must "never" end.
    #: Set it to false later, if for some reason the program must
    #: gracefully stop.
    _stop_consumer = True
    #: If we don't exit when we reach the sourcelog EOF,
    #: we'll wait this number of milliseconds before checking
    #: for new lines.
    _eof_wait = -1
    #: If set to False, signals cannot interrupt the program.
    _can_be_interrupted = True
    #: If set to True, the program will exit as soon as it is safe
    #: to do so.
    _should_stop = False

    #: Eventlog instance
    _eventlog = None
    #! Eventlog options distionary, to be passed to Eventlog
    _event_log_options = {
        # Truncate the Eventlog before starting
        'truncate': False
    }

    #: Type of log to consume, uppercase. Allowed values: ERROR, SLOW
    _sourcelog_type = None
    #: Path and name of the log to consume
    _sourcelog_path = None
    #: Past read line
    _sourcelog_last_position = None
    #: How many sourcelog entries will be processed as a maximum.
    #: Zero or a negative value means process them all
    _sourcelog_limit = None
    #: How many sourcelog entries will be skipped at the beginning.
    _sourcelog_offset = None

    #: GELF message we're composing and then sending to Graylog
    _message = None

    #: Necessary information to send messages to Graylog.
    _GRAYLOG = {
        # Graylog host
        'host': None,
        # Graylog port
        'port': None,
        # GELF version to use
        'GELF_version': '1.1'
    }

    # Misc

    _hostname = None


    ##  Methods
    ##  =======

    def __init__(self):
        """ The initialiser does nothing, so we have a complete instance before starting the real work. """
        pass

    def start(self):
        """ Start consuming the sourcelog. """
        import argparse

        # TODO: We should do this, to prevent multiple consumers to run at the same time
        #try:
        #    pidfile.PIDFile()
        #except pidfile.AlreadyRunningError:
        #    abort(1, 'PID file exists')

        # parse CLI arguments

        # Dictionary of arguments
        args = None
        # Object that handles CLI arguments
        arg_parser = None

        arg_parser = argparse.ArgumentParser(
            prog = Registry.PROGRAM,
            version = Registry.VERSION,
            description = Registry.DESCRIPTION
        )
        arg_parser.add_argument(
            '-t',
            '--log-type',
            required=True,
            help='Type of log to consume.'
        )
        arg_parser.add_argument(
            '-l',
            '--log',
            required=True,
            help='Path and name of the log file to consume.'
        )
        # --limit recalls SQL LIMIT
        arg_parser.add_argument(
            '--limit',
            type=int,
            default=-1,
            help='Maximum number of sourcelog entries to process. Zero or ' +
                'a negative value means process all sourcelog entries.' +
                'Implies --stop-never.'
        )
        # --offset recalls SQL LIMIT.
        # Since --limit doesn't have a short version, --offset doesn't neither.
        arg_parser.add_argument(
            '--offset',
            type=int,
            default=-1,
            help='Number of sourcelog entries to skip at the beginning. ' +
                'Zero or a negative value means skip nothing.'
        )
        # --stop-never is from mysqlbinlog
        # We have --stop=never, --stop=eof
        arg_parser.add_argument(
            '--stop',
            default=None,
            help='End the program never when it reaches the sourcelog EOF.'
        )
        # --*-wait is MariaDB stle. eof refers to --stop-eof.
        arg_parser.add_argument(
            '--eof-wait',
            type=int,
            default=1000,
            help='Number of milliseconds to wait after reaching the sourcelog' +
                'end, before checking if there are new contents.'
        )
        arg_parser.add_argument(
            '--label',
            default='',
            help='ID for the program execution. To calls with different ' +
                'IDs are allowed to run simultaneously. ' +
                'Default: same value as --log-type.'
        )
        # MariaDB tools use -h for the host they connect to
        # but with ArgParse it's used for --help, we we use
        # uppercase -H instead
        arg_parser.add_argument(
            '-H',
            '--graylog-host',
            default='',
            help='Graylog hostname.'
        )
        # MariaDB tools use -P for the port they connect to
        arg_parser.add_argument(
            '-P',
            '--graylog-port',
            type=int,
            help='Graylog UDP port.'
        )
        # Advertised name of the local host.
        # Shortened as -n because -h is already taken
        arg_parser.add_argument(
            '-n',
            '--hostname',
            help='Hostname as it will be sent to Graylog.'
        )
        # -t is used for --log-type. Also
        # -t in some tools stands for table, so we'll use -T.
        arg_parser.add_argument(
            '-T',
            '--truncate-eventlog',
            action='store_true',
            help='Truncate the eventlog before starting. Useful if the sourcelog was replaced.'
        )
        args = arg_parser.parse_args()

        # validate arguments

        if args.log.find(Eventlog.FIELD_SEPARATOR) > -1:
            abort(2, 'The source log name and path cannot contain the character: "' + Eventlog.FIELD_SEPARATOR + '"')

        if args.limit > -1 and (args.stop is not None and args.stop != 'limit'):
            abort(2, 'If --limit is > -1, --stop is set to \'limit\'')
        elif args.limit < 0 and args.stop == 'limit':
            abort(2, '--stop=limit is specified, but --limit is not specified')

        if args.label.find('/') > -1 or args.label.find('\\') > -1:
            abort(2, 'A label cannot contain slashes or backslashes')

        if (args.graylog_host and not args.graylog_port) or (args.graylog_port and not args.graylog_host):
            abort(2, 'Set both --graylog-host and --graylog-port, or none of them')

        # copy arguments into object members

        log_type = args.log_type.upper()
        if log_type == 'ERROR' or log_type == 'ERRORLOG':
            self._sourcelog_type = 'ERROR'
        elif log_type == 'SLOW' or log_type == 'SLOWLOG':
            self._sourcelog_type = 'SLOW'
        else:
            abort(2, 'Invalid value for --log-type')
        del log_type
        self._sourcelog_path = str(args.log)
        self._sourcelog_limit = args.limit - 1
        self._sourcelog_offset = args.offset - 1
        # --limit implies a program stop
        if args.limit > -1:
            self._stop = 'limit'
        elif args.stop is not None:
            self._stop = args.stop
        else:
            # default when --limit is absent
            self._stop = 'never'
        self._eof_wait = args.eof_wait
        if args.label:
            self._label = args.label
        else:
            self._label = args.log_type

        self._GRAYLOG['host'] = args.graylog_host
        self._GRAYLOG['port'] = args.graylog_port

        try:
            self.log_handler = open(self._sourcelog_path, 'r', 0)
        except:
            abort(2, 'Could not open source log: ' + self._sourcelog_path)

        if args.hostname:
            self._hostname = args.hostname
        else:
            self._hostname = self._get_hostname()

        if args.truncate_eventlog:
            self._event_log_options['truncate'] = True

        # cleanup the CLI parser

        del args
        del arg_parser

        try:
            self._eventlog = Eventlog(self._event_log_options)
        except Exception as e:
            abort(3, str(e))

        # Note: we want to start handling signals before creating the lock file
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

        self._lock_file_name = self._LOCK_FILE_PATH + '/' + self._label
        try:
            self._lock_file = self.os.open(self._lock_file_name, self.os.O_CREAT | self.os.O_EXCL | self.os.O_RDWR)
        except OSError:
            abort(3, 'Lock file exists or cannot be created: ' + self._lock_file_name)

        self.consuming_loop()

    def _get_timestamp(self):
        """ Return UNIX timestamp (not decimals) """
        return str( int ( self.time.time() ) )

    def _get_hostname(self):
        """ Used to set the hostname for the first time """
        import socket
        return socket.gethostname()

    def _get_current_position(self):
        """ Get the position that we're currently reading """
        return str(self.log_handler.tell())

    def _log_coordinates(self):
        """ Log last consumed coordinates """
        self._sourcelog_last_position = self._get_current_position()
        self._eventlog.append(self._get_current_position(), self._sourcelog_path)

    def cleanup(self):
        """ Do the cleanup and terminate program execution """
        self._eventlog.close()
        self.os.close(self._lock_file)
        self.os.unlink(self._lock_file_name)
        sys.exit(0)

    def handle_signal(self, signum, frame):
        """ Handle signals to avoid that the program is interrupted when it shouldn't be. """
        if self._can_be_interrupted:
            self.cleanup()
        else:
            self._should_stop = True


    ##  Consumer Loop
    ##  =============

    def _get_next_word(self, line, offset=0, to_end=False):
        """ Generic method to get the next word from a line, or None """
        index = 0
        word = ''
        word_started = False

        for char in line:
            index += 1

            # do nothing til we consume the offset
            if index < offset:
                continue

            if char.isspace() and to_end == False:
                # Space characters are separators, so
                # ignore them before the word starts
                # and end the loop if word was started.
                # Also, ignore them if to_end==True, the loop
                # will continue til the end of the line.
                if word_started:
                    break
                else:
                    continue

            word_started = True
            word += char

        if to_end == True:
            word = word.strip()

        return {
            "word": word,
            "index": index
        }

    def extract_word(self, next_word):
        """ Extract the word from a next_word fictionary, handle errors """
        try:
            word = next_word['word']
        except:
            abort(1, 'Malformed next_word dictionary: ' + str(next_word))
        return word

    def _process_message(self):
        """ Send the message and log the coordinates.
            Prevent the program to be interrupted just before sending
            the message and release the protection after logging.
        """
        self._can_be_interrupted = False
        self._message.send()
        self._message = None
        self._log_coordinates()
        self._can_be_interrupted = True

        if self._should_stop:
            self.cleanup()

    def consuming_loop(self):
        """ Consumer's main loop, in which we read next lines if available, or wait for more lines to be written.
            Calls a specific method based on _sourcelog_type.
        """
        if self._sourcelog_type == 'ERROR':
            self.error_log_consuming_loop()
        else:
            self.slow_log_consuming_loop()


    ##  Error Log
    ##  =========

    def error_log_process_line(self, line):
        """ Process a line from the Error Log, extract information, compose a GELF message if necessary """
        well_formed = True

        next_word = self._get_next_word(line)
        date_part = next_word['word']

        next_word = self._get_next_word(line, next_word['index'])
        time_part = next_word['word']

        # We need to support the following line formats.
        # Format 1:
        # 2019-11-01 16:10:48 0 [Note] WSREP: Read nil XID from storage engines, skipping position init
        # Format 2:
        # 201030 12:40:21 [ERROR] mysqld got signal 6 ;

        # Assigning meaningful values to these variables will fail if
        # no known format is detected
        time_list = None
        date_time = None
        timestamp = None

        try:
            # First we'll try to get date and time, to find out if the row is well-formed.
            # If it is not, it is a continuation of the previous line, so we merge it
            # to its message.
            # If it is, we can consider the previous line complete, so we send a GELF message
            # (if a message is prepared; otherwise, it is the first line and we have nothing to send).

            # Format 1
            # We are doing this to zeropad the "hour" part.
            # We could just zeropad time_part, but we want to be flexible in case we need to add
            # a microsecond part.
            time_list = time_part.split(':')
            date_time = date_part + ' ' + time_list[0].zfill(2) + ':' + time_list[1].zfill(2) + ':' + time_list[2].zfill(2)

            date_time = self.datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").timetuple()
            timestamp = int(self.time.mktime(date_time))

            next_word = self._get_next_word(line, next_word['index'])
            thread = next_word['word']

            next_word = self._get_next_word(line, next_word['index'])
            level = next_word['word']

            next_word = self._get_next_word(line, next_word['index'], True)
            message = next_word['word']
        except (ValueError, IndexError) as e:
            try:
                # Format 2
                time_list = time_part.split(':')
                date_time = date_part + ' ' + time_list[0].zfill(2) + ':' + time_list[1].zfill(2) + ':' + time_list[2].zfill(2)

                date_time = self.datetime.datetime.strptime(date_time, "%y%m%d %H:%M:%S").timetuple()
                timestamp = int(self.time.mktime(date_time))

                next_word = self._get_next_word(line, next_word['index'])
                level = next_word['word']

                next_word = self._get_next_word(line, next_word['index'], True)
                message = next_word['word']
            except (ValueError, IndexError) as e:
                # No known format was detected
                well_formed = False
                next_word = self._get_next_word(line, offset=0, to_end=True)
                message = next_word['word']

        del time_list
        del date_time

        if well_formed:
            # A new message starts with this line.
            # If it is not a first (IE, a message was already composed)
            # send the composed message.
            if self._message:
                self._process_message()

            # Start to compose the new message

            # to increase format changes resilience, remove brackets and make uppercase
            level = level          \
                .replace('[', '')  \
                .replace(']', '')  \
                .upper()

            custom = {
                "text": message
            }

            self._message = GELF_message(
                    Registry.DEBUG,
                    self._GRAYLOG['GELF_version'],
                    timestamp,
                    self._hostname,
                    'short',
                    level,
                    custom
                )

            if Registry.DEBUG['LOG_LINES']:
                print(line)
            if Registry.DEBUG['LOG_PARSER']:
                print(str(next_word))

        # Not well-formed. Append the line to the existing message
        # _text property.
        else:
            if Registry.DEBUG['LOG_PARSER']:
                print('Processing multiline message')
            self._message.append_to_field(True, 'text', message)

    def error_log_consuming_loop(self):
        """ Consumer's main loop for the Error Log """

        # if an offset was read from the Eventlog on start,
        # skip to the offset
        if self._eventlog.get_offset() is not None:
            self.log_handler.seek(self._eventlog.get_offset())

        while True:

            source_line = self.log_handler.readline().rstrip()
            while (source_line):
                # if _sourcelog_offset is not negative, skip this line,
                # read the next and decrement
                if self._sourcelog_offset > -1:
                    self._sourcelog_offset = self._sourcelog_offset - 1
                    source_line = self.log_handler.readline()
                    continue
                self.error_log_process_line(source_line)
                source_line = self.log_handler.readline()
                # enforce --limit if it is > -1
                if self._sourcelog_limit == 0:
                    break
                elif self._sourcelog_limit > 0:
                    self._sourcelog_limit = self._sourcelog_limit - 1

            if self._message:
                self._process_message()

            # We reached sourcelog EOF.
            # Depening on _stop, we exit the loop (and then the program)
            # or we wait a given interval and repeat the loop.
            if self._stop == 'limit' or self._stop == 'eof':
                break
            self.time.sleep(self._eof_wait / 1000)


    ##  Slow Log
    ##  ========

    def slow_log_process_line(self, line):
        """ Process a line from the Error Log, extract information, compose a GELF message if necessary """
        print(line)

    def slow_log_consuming_loop(self):
        """ Consumer's main loop for the Slow log """
        source_line = self.log_handler.readline().rstrip()
        while (source_line):
            self.slow_log_process_line(source_line)
            source_line = self.log_handler.readline().rstrip()


def abort(return_code, message):
    """ Abort the program with specified return code and error message """
    if Registry.consumer:
        # When an anomaly occurs, the consumer object
        # may not have been created yet
        try:
            Registry.consumer.cleanup()
        except:
            pass
    if message:
        print(message)
    sys.exit(return_code)


if __name__ == '__main__':
    Registry.consumer = Consumer()
    Registry.consumer.start()

#EOF