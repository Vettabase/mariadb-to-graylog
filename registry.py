#!/usr/bin/env python


""" This module only contains a Registry class, which contains global (program-level)
    constants and variables.
"""


class Registry:
    '''
    Global constants and variables (Registry pattern)
    '''


    ##  Global Constants
    ##  ================

    #: Program name
    PROGRAM = 'MariaDB To Graylog'
    #: Program version
    VERSION = '0.1'
    #: Program description
    DESCRIPTION = 'Consume MariaDB error & slow logs and send them to GrayLog'

    #: Backup flags, for additional output
    DEBUG = {
        # Print GELF messages before sending them
        'GELF_MESSAGES': True,
        # Print read log lines
        'LOG_LINES': False,
        # Print info about parsed log lines
        'LOG_PARSER': False
    }


    ##  Global Variables
    ##  ================

    #: Consumer instance
    consumer = None

#EOF