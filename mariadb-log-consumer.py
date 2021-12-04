#!/usr/bin/env python


import sys
import signal


consumer = None


class Consumer:
    """
    Flexible metaclass for defining useful decorator functions.
    """

    ##  Modules
    ##  =======

    import time
    import os
    #import pidfile


    ##  Members
    ##  =======

    # Type of log to consume, uppercase. Allowed values: ERROR, SLOW
    sourcelog_type = None
    # Path and name of the log to consume
    sourcelog_path = None
    # Log file handler, watched
    sourcelog_handler = None


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

    EVENTLOG_PATH = '/var/mariadb-to-graylog/logs'
    EVENTLOG_NAME = 'events.log'

    eventlog_handler = None
    last_position = None


    # Graylog
    #
    # We'll send GELF messages to Graylog via UDP.

    GELF_VERSION = '1.1'

    graylog_host = None
    graylog_port = None


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
            prog='mariadb-log-consumer',
            version='0.1',
            description='Consume logs and send them to GrayLog'
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
        arg_parser.add_argument(
            '-H',
            '--graylog-host',
            default='',
            help='Graylog hostname'
        )
        arg_parser.add_argument(
            '-P',
            '--graylog-port',
            type=int,
            help='Graylog UDP port'
        )
        arg_parser.add_argument(
            '-n',
            '--hostname',
            help='Hostname as it will be sent to Graylog'
        )
        args = arg_parser.parse_args()

        # copy arguments into object members

        self.sourcelog_type = args.log_type.upper()
        self.sourcelog_path = str(args.log)

        self.graylog_host = args.graylog_host
        self.graylog_port = args.graylog_port

        if (self.graylog_host and not self.graylog_port) or (self.graylog_port and not self.graylog_host):
            abort(2, 'Set both --graylog-host and --graylog-port, or none of them')

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
        self.open_eventlog()
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

    def get_eventlog_name(self):
        """ Compose the file for a new eventlog """
        return self.EVENTLOG_PATH + '/' + self.EVENTLOG_NAME

    def open_eventlog(self):
        """ Open newest log file. If the file is changed (eg by logrotate) it closes and reopens it. """
        import logging
        import logging.handlers
        eventlog_file = self.get_eventlog_name()
        try:
            self.eventlog_handler = logging.handlers.WatchedFileHandler(eventlog_file)
        except:
            abort(3, 'Could not open or create eventlog: ' + eventlog_file)
        self.eventlog_handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        self.eventlog_handler.setLevel(logging.INFO)
        root = logging.getLogger()
        root.addHandler(self.eventlog_handler)
        self.logging = logging

    def get_current_position(self):
        """ Get the position that we're currently reading """
        return str(self.log_handler.tell())

    def log_coordinates(self, action):
        """ Log last read coordinates and whether the last rows were sent or not """
        self.last_position = self.get_current_position()
        self.logging.info(action + ':' + self.get_current_position() + ':' + self.EVENTLOG_PATH)

    def cleanup(self):
        """ Do the cleanup before execution terminates """
        self.eventlog_handler.close()


    ##  GELF Messages
    ##  =============

    def get_gelf_field(self, key, value):
        """ Compose a single key/value couple in a GELF line"""
        value = value.replace('\\', '\\\\')
        return '"' + key + '":"' + value + '"'

    def get_gelf_line(
            self,
            # Mandatory GELF properties
            host,
            short_message,
            level,
            # Custom properties
            extra={ }
        ):
        """ Compose a line of GELF metrics for Graylog.
            GELF documentation:
            https://docs.graylog.org/docs/gelf
        """
        message = '{'

        message += self.get_gelf_field('version', self.GELF_VERSION)
        # The hostname was set previously
        message += ',' + self.get_gelf_field('host', self.get_hostname())
        # 'MariaDB Error Log' or 'MariaDB Slow Log'
        message += ',' + self.get_gelf_field('short_message', short_message)
        message += ',' + self.get_gelf_field('timestamp', self.get_timestamp() )
        # Same levels as syslog:
        # 0=Emergency, 1=Alert, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Informational, 7=Debug
        # https://docs.delphix.com/docs534/system-administration/system-monitoring/setting-syslog-preferences/severity-levels-for-syslog-messages
        message += ',' + self.get_gelf_field('level', level)

        # all custom fields (not mentioned in GELF specs)
        # must start with a '_'
        for key in extra:
            message += self.get_gelf_field('_' + key, extra[key])

        message += '}'

        return message


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
        next_word = self.get_next_word(line)
        date = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        time = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        thread = next_word['word']

        next_word = self.get_next_word(line, next_word['index'])
        level = next_word['word']

        next_word = self.get_next_word(line, next_word['index'], True)
        message = next_word['word']

        gelf_message = self.get_gelf_line(self.hostname, 'short', 'lev');

        print(str(next_word))
        print(gelf_message)
        print(line)

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
    consumer.cleanup()
    sys.exit(0)


def abort(return_code, message):
    """ Abort the program with specified return code and error message """
    if consumer:
        consumer.cleanup()
    if message:
        print(message)
    sys.exit(return_code)


if __name__ == '__main__':
    consumer = Consumer()
