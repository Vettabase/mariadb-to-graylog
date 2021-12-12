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

    # Type of log to consume, uppercase. Allowed values: ERROR, SLOW
    sourcelog_type = None
    # Path and name of the log to consume
    sourcelog_path = None
    # Log file handler, watched
    sourcelog_handler = None
    # last read line
    sourcelog_last_position = None


    # Necessary information to send messages to Graylog.
    GRAYLOG = {
        # Graylog host
        'host': None,
        # Graylog port
        'port': None,
        # GELF version to use
        'GELF_version': '1.1'
    }


    # Misc

    hostname = None


    ##  Methods
    ##  =======

    def __init__(self):
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
        args = arg_parser.parse_args()

        # validate arguments

        if args.log.find(Eventlog.FIELD_SEPARATOR) > -1:
            abort(2, 'The source log name and path cannot contain the character: "' + Eventlog.FIELD_SEPARATOR + '"')

        if (args.graylog_host and not args.graylog_port) or (args.graylog_port and not args.graylog_host):
            abort(2, 'Set both --graylog-host and --graylog-port, or none of them')

        # copy arguments into object members

        self.sourcelog_type = args.log_type.upper()
        self.sourcelog_path = str(args.log)

        self.GRAYLOG['host'] = args.graylog_host
        self.GRAYLOG['port'] = args.graylog_port

        try:
            self.log_handler = open(self.sourcelog_path, 'r', 0)
        except:
            abort(2, 'Could not open source log: ' + self.sourcelog_path)

        if (args.hostname):
            self.hostname = args.hostname
        else:
            self.hostname = self.get_hostname()

        # cleanup the CLI parser

        args = None
        arg_parser = None

        self.register_signal_handlers()
        self.eventlog = Eventlog()
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

    def log_coordinates(self, action):
        """ Log last read coordinates and whether the last rows were sent or not """
        self.sourcelog_last_position = self.get_current_position()
        self.eventlog.append(action, self.get_current_position(), self.sourcelog_path)

    def cleanup(self):
        """ Do the cleanup before execution terminates """
        self.eventlog.close()


    ##  Consumer Loop
    ##  =============

    def get_next_word(self, line, offset=0, to_end=False):
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
            Calls a specific method based on sourcelog_type.
        """
        if self.sourcelog_type == 'ERROR':
            self.error_log_consuming_loop()
        else:
            self.slow_log_consuming_loop()
        self.log_coordinates('READ')


    ##  Error Log
    ##  =========

    def error_log_process_line(self, line):
        """ Process a line from the Error Log, extract information, compose a GELF message if necessary """
        well_formed = True

        next_word = self.get_next_word(line)
        date_part = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        time_part = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        thread = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        level = next_word['word']

        next_word = self.get_next_word(line, next_word['index'], True)
        message = next_word['word']

        # @TODO: This is what we *should* do. Make it so.
        # First we'll try to get date and time, to find out if the row is well-formed.
        # If it is not, it is a continuation of the previous line, so we merge it
        # to its message.
        # If it is, we can consider the previous line complete, so we send a GELF message.

        # We are doing this to zeropad the "hour" part.
        # We could just zeropad time_part, but we want to be flexible in case we need to add
        # a microsecond part.
        date_time = None
        try:
            time_list = time_part.split(':')
            date_time = date_part + ' ' + time_list[0].zfill(2) + ':' + time_list[1].zfill(2) + ':' + time_list[2].zfill(2)
        except:
            well_formed = False

        if well_formed:
            date_time = self.datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").timetuple()
            timestamp = int(self.time.mktime(date_time))

            # to increase format changes resilience, remove brackets and make uppercase
            level = level          \
                .replace('[', '')  \
                .replace(']', '')  \
                .upper()

            custom = {
                "text": message
            }

            message = GELF_message(
                    Registry.DEBUG,
                    self.GRAYLOG['GELF_version'],
                    timestamp,
                    self.get_hostname(),
                    'short',
                    level,
                    custom
                )

            if Registry.DEBUG['LOG_LINES']:
                print(line)
            if Registry.DEBUG['LOG_PARSER']:
                print(str(next_word))

            message.send()

    def error_log_consuming_loop(self):
        """ Consumer's main loop for the Error Log """
        source_line = self.log_handler.readline().rstrip()
        while (source_line):
            self.error_log_process_line(source_line)
            source_line = self.log_handler.readline().rstrip()
        self.log_coordinates('READ')


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
        self.log_coordinates('READ')


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

#EOF