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

    # Eventlog instance
    eventlog = None
    # Truncate the Eventlog before starting
    _event_log_options = {
        'truncate': False
    }

    # Type of log to consume, uppercase. Allowed values: ERROR, SLOW
    _sourcelog_type = None
    # Path and name of the log to consume
    _sourcelog_path = None
    # Past read line
    _sourcelog_last_position = None
    # How many sourcelog entries will be processed as a maximum.
    # Zero or a negative value means process them all
    _sourcelog_limit = None
    # How many sourcelog entries will be skipped at the beginning.
    _sourcelog_offset = None

    #: GELF message we're composing and then sending to Graylog
    _message = None

    # Necessary information to send messages to Graylog.
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

        arg_parser = argparse.ArgumentParser(
            prog = Registry.PROGRAM,
            version = Registry.VERSION,
            description = Registry.DESCRIPTION
        )
        arg_parser.add_argument(
            '-t',
            '--log-type',
            choices=['error', 'slow'],
            required=True,
            help='Type of log to consume'
        )
        arg_parser.add_argument(
            '-l',
            '--log',
            required=True,
            help='Path and name of the log file to consume'
        )
        # --limit recalls SQL LIMIT
        arg_parser.add_argument(
            '--limit',
            type=int,
            default=-1,
            help='Maximum number of sourcelog entries to process. Zero or ' +
                'a negative value means process all sourcelog entries.'
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
        # MariaDB tools use -h for the host they connect to
        # but with ArgParse it's used for --help, we we use
        # uppercase -H instead
        arg_parser.add_argument(
            '-H',
            '--graylog-host',
            default='',
            help='Graylog hostname'
        )
        # MariaDB tools use -P for the port they connect to
        arg_parser.add_argument(
            '-P',
            '--graylog-port',
            type=int,
            help='Graylog UDP port'
        )
        # Advertised name of the local host.
        # Shortened as -n because -h is already taken
        arg_parser.add_argument(
            '-n',
            '--hostname',
            help='Hostname as it will be sent to Graylog'
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

        if (args.graylog_host and not args.graylog_port) or (args.graylog_port and not args.graylog_host):
            abort(2, 'Set both --graylog-host and --graylog-port, or none of them')

        # copy arguments into object members

        self._sourcelog_type = args.log_type.upper()
        self._sourcelog_path = str(args.log)
        self._sourcelog_limit = args.limit - 1
        self._sourcelog_offset = args.offset - 1

        self._GRAYLOG['host'] = args.graylog_host
        self._GRAYLOG['port'] = args.graylog_port

        try:
            self.log_handler = open(self._sourcelog_path, 'r', 0)
        except:
            abort(2, 'Could not open source log: ' + self._sourcelog_path)

        if (args.hostname):
            self._hostname = args.hostname
        else:
            self._hostname = self.get_hostname()

        if args.truncate_eventlog:
            self._event_log_options['truncate'] = True

        # cleanup the CLI parser

        args = None
        arg_parser = None

        self.register_signal_handlers()
        try:
            self.eventlog = Eventlog(self._event_log_options)
        except Exception as e:
            abort(3, str(e))
        self.consuming_loop()

    def register_signal_handlers(self):
        """ Register system signal handlers """
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

    def get_timestamp(self):
        """ Return UNIX timestamp (not decimals) """
        return str( int ( self.time.time() ) )

    def get_hostname(self):
        """ Used to set the hostname for the first time """
        import socket
        return socket.gethostname()

    def get_current_position(self):
        """ Get the position that we're currently reading """
        return str(self.log_handler.tell())

    def log_coordinates(self):
        """ Log last consumed coordinates """
        self._sourcelog_last_position = self.get_current_position()
        self.eventlog.append(self.get_current_position(), self._sourcelog_path)

    def cleanup(self):
        """ Do the cleanup before execution terminates """
        self.eventlog.close()


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

        if well_formed:
            # A new message starts with this line.
            # If it is not a first (IE, a message was already composed)
            # send the composed message.
            if self._message:
                self._message.send()
                self._message = None
                self.log_coordinates()

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
        if self.eventlog.get_offset() is not None:
            self.log_handler.seek(self.eventlog.get_offset())

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
            self._message.send()
            self._message = None
            self.log_coordinates()


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


def shutdown(sig, frame):
    """ Terminate the program normally """
    Registry.consumer.cleanup()
    sys.exit(0)


def abort(return_code, message):
    """ Abort the program with specified return code and error message """
    if Registry.consumer:
        Registry.consumer.cleanup()
    if message:
        print(message)
    sys.exit(return_code)


if __name__ == '__main__':
    Registry.consumer = Consumer()
    Registry.consumer.start()

#EOF