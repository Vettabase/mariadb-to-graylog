#!/usr/bin/env python3


import sys
import signal

from lib_consumer import Graylog_Client
from lib_consumer import *
from registry import Registry


class Consumer:
    """ Main class of the program.
        Handles CLI arguments and starts a consumer for MariaDB
        error log and slow log.
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
    #: If True, checks on the lock file are disabled.
    _force_run = False
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
    #: Requests from signals that cannot be accomplished immediately
    #: are stored here.
    _requests = Request_Counters(('STOP', 'ROTATE'))

    _message_wait = None

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

    #: Necessary information to send messages to work with Graylog.
    _GRAYLOG = {
        # Graylog client objects, that contain all necessary
        # information to connect to Graylog via UDP and TCP
        'client_udp': None,
        'client_tcp': None,
        'client_http': None,
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
            #version = Registry.VERSION,
            description = Registry.DESCRIPTION,
            epilog =
                'Exit codes:\n' +
                '    0  Success\n' +
                '    1  Generic error\n' +
                '    2  Invalid input\n' +
                '    3  OS error'
                ,
            formatter_class = argparse.RawTextHelpFormatter
        )
        arg_parser.add_argument(
            '-t',
            '--log-type',
            required=True,
            help='Type of log to consume. Permitted values: error, slow.\n' +
                'Permitted aliases: errorlog, errorlog. Case-insensitive.'
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
            help='Maximum number of sourcelog entries to process. Zero or\n' +
                'a negative value means process all sourcelog entries.\n' +
                'Implies --stop-never.'
        )
        # --offset recalls SQL LIMIT.
        # Since --limit doesn't have a short version, --offset doesn't neither.
        arg_parser.add_argument(
            '--offset',
            type=int,
            default=-1,
            help='Number of sourcelog entries to skip at the beginning.\n' +
                'Zero or a negative value means skip nothing.'
        )
        # --stop-never is from mysqlbinlog
        # We have --stop=never, --stop=eof
        arg_parser.add_argument(
            '--stop',
            default=None,
            help='When the program must stop. Allowed values:\n' +
                '    eof:    When the end of file is reached.\n' +
                '    limit:  When --limit sourcelog entries are processed.\n' +
                '    never:  Always keep running, waiting for new\n' +
                '            entries to process.'
        )
        # --*-wait is MariaDB style. eof refers to --stop=eof.
        arg_parser.add_argument(
            '--eof-wait',
            type=int,
            default=1000,
            help='Number of milliseconds to wait after reaching the sourcelog\n' +
                'end, before checking if there are new contents.'
        )
        # --*-wait is MariaDB style
        arg_parser.add_argument(
            '--message-wait',
            type=int,
            default=0,
            help='Number of milliseconds to wait before processing the\n' +
                'next message, as a trivial mechanism to avoid overloading\n' +
                'the server.'
        )
        arg_parser.add_argument(
            '--label',
            default='',
            help='ID for the program execution. To calls with different\n' +
                'IDs are allowed to run simultaneously.\n' +
                'Default: same value as --log-type.'
        )
        arg_parser.add_argument(
            '-f',
            '--force-run',
            action='store_true',
            help='Don\'t check if another instance of the program is\n' +
                'running, and don\'t prevent other instances from running.'
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
        arg_parser.add_argument(
            '--graylog-port-udp',
            type=int,
            help='Graylog UDP port.'
        )
        arg_parser.add_argument(
            '--graylog-port-tcp',
            type=int,
            help='Graylog TCP port.'
        )
        arg_parser.add_argument(
            '--graylog-port-http',
            type=int,
            help='Graylog HTTP port.'
        )
        # TCP options
        arg_parser.add_argument(
            '--graylog-tcp-timeout',
            type=int,
            default=2,
            help='Timeout for TCP calls.'
        )
        # HTTP options
        arg_parser.add_argument(
            '--graylog-http-timeout-idle',
            type=int,
            default=5,
            help='Timeout for the HTTP call when no data is received.'
        )
        arg_parser.add_argument(
            '--graylog-http-timeout',
            type=int,
            default=10,
            help='Timeout for the HTTP call. This is a hard limit.'
        )
        arg_parser.add_argument(
            '--graylog-http-max-retries',
            type=int,
            default=None,
            help='Max attempts for HTTP requests.'
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
            help='Truncate the eventlog before starting. Useful if the\n' +
                'sourcelog was replaced.'
        )
        args = arg_parser.parse_args()

        # validate arguments

        if args.log.find(Eventlog.FIELD_SEPARATOR) > -1:
            abort(2, 'The sourcelog name and path cannot contain the character: "' + Eventlog.FIELD_SEPARATOR + '"')

        if args.stop is not None:
            args.stop = args.stop.upper()
        if args.limit > -1 and (args.stop is not None and args.stop != 'LIMIT'):
            abort(2, 'If --limit is > -1, --stop is set to \'limit\'')
        elif args.limit < 0 and args.stop == 'LIMIT':
            abort(2, '--stop=limit is specified, but --limit is not specified')
        elif args.stop:
            if args.stop not in ('NEVER', 'EOF', 'LIMIT'):
                abort(2, 'Invalid value for --stop: ' + args.stop)

        if args.label.find('/') > -1 or args.label.find('\\') > -1:
            abort(2, 'A label cannot contain slashes or backslashes')

        if bool(args.graylog_host) != (bool(args.graylog_port_udp) or bool(args.graylog_port_tcp) or bool(args.graylog_port_http)):
            abort(2, 'Set --graylog-host and at least one port, or omit all these options')

        if args.graylog_http_max_retries is not None and args.graylog_http_max_retries < 0:
            abort(2, '--graylog-http-max-retries can only be a non-negative integer')

        # copy arguments into object members

        log_type = args.log_type.upper()
        if log_type == 'ERROR' or log_type == 'ERRORLOG':
            self._sourcelog_type = 'ERROR'
        elif log_type == 'SLOW' or log_type == 'SLOWLOG':
            self._sourcelog_type = 'SLOW'
        else:
            abort(2, 'Invalid value for --log-type')
        del log_type
        self._message_wait = args.message_wait
        self._sourcelog_path = str(args.log)
        self._sourcelog_limit = args.limit - 1
        self._sourcelog_offset = args.offset - 1
        # --limit implies a program stop
        if args.limit > -1:
            self._stop = 'LIMIT'
        elif args.stop is not None:
            self._stop = args.stop
        else:
            # default when --limit is absent
            self._stop = 'NEVER'
        self._eof_wait = args.eof_wait
        if args.force_run:
            self._force_run = True
        if args.label:
            self._label = args.label
        else:
            self._label = args.log_type

        # host and port information will only be stored in Graylog client
        if args.graylog_port_udp:
            self._GRAYLOG['client_udp'] = Graylog_Client_UDP(
                args.graylog_host,
                args.graylog_port_udp
            )
        if args.graylog_port_tcp:
            self._GRAYLOG['client_tcp'] = Graylog_Client_TCP(
                args.graylog_host,
                args.graylog_port_tcp,
                args.graylog_tcp_timeout
            )
        if args.graylog_port_http:
            self._GRAYLOG['client_http'] = Graylog_Client_HTTP(
                args.graylog_host,
                args.graylog_port_http,
                args.graylog_http_timeout_idle,
                args.graylog_http_timeout,
                args.graylog_http_max_retries
            )

        try:
            self.log_handler = open(self._sourcelog_path, 'r')
        except:
            abort(2, 'Could not open sourcelog: ' + self._sourcelog_path)

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
        signal.signal(signal.SIGHUP, self.handle_signal)

        if not self._force_run:
            self._lock_file_name = self._LOCK_FILE_PATH + '/' + self._label
            try:
                self._lock_file = self.os.open(self._lock_file_name, self.os.O_CREAT | self.os.O_EXCL | self.os.O_RDWR)
            except OSError:
                abort(3, 'Lock file exists or cannot be created: ' + self._lock_file_name)

        self._consuming_loop()

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

    def cleanup(self, exit_program=True):
        """ Do the cleanup and terminate program execution """
        self._eventlog.close()
        if not self._force_run:
            self.os.close(self._lock_file)
            self.os.unlink(self._lock_file_name)
            # Destructors will close the connections where necessary
            del self._GRAYLOG['client_tcp']
        if exit_program:
            sys.exit(0)

    def handle_signal(self, signum, frame):
        """ Handle signals to avoid that the program is interrupted when it shouldn't be. """
        if self._can_be_interrupted:
            if signum == signal.SIGHUP:
                self._eventlog.rotate()
            else:
                self.cleanup()
        else:
            if signum == signal.SIGHUP:
                self._requests.increment('ROTATE')
            elif signum == signal.SIGINT or signum == signal.SIGTERM:
                self._requests.increment('STOP')


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

    def _disallow_interruptions(self):
        """ Prevent the program from being interrupted
            until _allow_interruptions() is called.
            Idempotent.
        """
        self._can_be_interrupted = False

    def _allow_interruptions(self):
        """ Allow the program to be interrupted from now on.
            Check if there are pending requests, and handle them.
            Idempotent.
        """
        self._can_be_interrupted = True

        if self._requests.was_requested('STOP'):
            self.cleanup()
        elif self._requests.was_requested('ROTATE'):
            self._eventlog.rotate()

    def _process_message(self):
        """ Send the message and log the coordinates.
            Prevent the program to be interrupted just before sending
            the message and release the protection after logging.
        """
        message_string = self._message.to_string()

        if Registry.DEBUG['GELF_MESSAGES']:
            print(message_string)

        self._disallow_interruptions()

        sent = False

        if self._GRAYLOG['client_udp']:
            try:
                self._GRAYLOG['client_udp'].send(
                    bytearray(message_string, 'us-ascii')
                )
                sent = True
            except:
                pass
        
        if sent == False and self._GRAYLOG['client_tcp']:
            try:
                self._GRAYLOG['client_tcp'].send(
                    bytearray(message_string, 'us-ascii')
                )
                sent = True
            except:
                pass

        if sent == False and 'client_http' in self._GRAYLOG:
            try:
                self._GRAYLOG['client_http'].send(
                    message_string
                )
            except:
                pass

        self._message = None
        self._log_coordinates()

        self._allow_interruptions()


    def _consuming_loop(self):
        """ Consumer's main loop, in which we read next lines if available, or wait for more lines to be written.
            Calls a specific method based on _sourcelog_type.
        """
        if Registry.DEBUG['DODGE_EXCEPTIONS'] == True:
            if self._sourcelog_type == 'ERROR':
                self._error_log_consuming_loop()
            else:
                self._slow_log_consuming_loop()
        else:
            try:
                if self._sourcelog_type == 'ERROR':
                    self._error_log_consuming_loop()
                else:
                    self._slow_log_consuming_loop()
            except Exception as x:
                self.cleanup(False)
                raise x


    ##  Error Log
    ##  =========

    def _error_log_process_line(self, line):
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
            # If it is not the first message (IE, a message was already composed)
            # send the last composed message.
            if self._message:
                self._process_message()

            # Start to compose the new message

            short_message = level + ' ' + message[:Registry.SHORT_MESSAGE_LENGTH]

            # to increase format changes resilience, remove brackets and make uppercase
            level = level          \
                .replace('[', '')  \
                .replace(']', '')  \
                .upper()

            custom = {
                "text": message
            }

            self._message = GELF_Message(
                    Registry.DEBUG,
                    self._GRAYLOG['GELF_version'],
                    timestamp,
                    self._hostname,
                    short_message,
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
            #self._message.append_to_field(True, 'text', message)

    def _get_source_line(self):
        """ Return processed next line from the sourcelog,
            after waiting _message_wait as appropriate.
        """

    def _error_log_consuming_loop(self):
        """ Consumer's main loop for the Error Log """

        # if an offset was read from the Eventlog on start,
        # skip to the offset
        if self._eventlog.get_offset():
            self.log_handler.seek(self._eventlog.get_offset())

        # set to True when repeating a loop,
        # to indicate that we may need to wait _message_wait msecs
        # to avoid processing messages to quickly
        in_sequence = False

        while True:
            source_line = self.log_handler.readline().rstrip()
            while source_line:
                # if _sourcelog_offset is not negative, skip this line,
                # read the next and decrement
                if self._sourcelog_offset > -1:
                    self._sourcelog_offset = self._sourcelog_offset - 1
                    source_line = self.log_handler.readline()
                    continue

                in_sequence = True

                if self._message_wait:
                    self.time.sleep(self._message_wait / 1000)
                self._error_log_process_line(source_line)
                source_line = self.log_handler.readline().rstrip()

                # enforce --limit if it is > -1
                if self._sourcelog_limit == 0:
                    break
                elif self._sourcelog_limit > 0:
                    self._sourcelog_limit = self._sourcelog_limit - 1

            if self._message_wait:
                self.time.sleep(self._message_wait / 1000)
            if self._message:
                self._process_message()

            in_sequence = False

            # We reached sourcelog EOF.
            # Depening on _stop, we exit the loop (and then the program)
            # or we wait a given interval and repeat the loop.
            if self._stop == 'LIMIT' or self._stop == 'EOF':
                break
            if self._eof_wait > 0:
                self.time.sleep(self._eof_wait / 1000)

        self.cleanup()


    ##  Slow Log
    ##  ========

    def _slow_log_process_line(self, line):
        """ Process a line from the Error Log, extract information, compose a GELF message if necessary """
        print(line)

    def _slow_log_consuming_loop(self):
        """ Consumer's main loop for the Slow log """
        while True:
            source_line = self.log_handler.readline()
            while source_line:
                self._slow_log_process_line(source_line)
                source_line = self.log_handler.readline()

                # enforce --limit if it is > -1
                if self._sourcelog_limit == 0:
                    break
                elif self._sourcelog_limit > 0:
                    self._sourcelog_limit = self._sourcelog_limit - 1

            # We reached sourcelog EOF.
            # Depening on _stop, we exit the loop (and then the program)
            # or we wait a given interval and repeat the loop.
            if self._stop == 'LIMIT' or self._stop == 'EOF':
                break
            if self._eof_wait > 0:
                self.time.sleep(self._eof_wait / 1000)

        self.cleanup()


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