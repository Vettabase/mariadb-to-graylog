#!/usr/bin/env python


""" This module only contains a Registry class, which contains global (program-level)
    constants and variables.
"""


class Registry:
    """
    Global constants and variables (Registry pattern)
    """


    ##  Global Constants
    ##  ================

    PROGRAM = 'MariaDB To Graylog'
    VERSION = '0.1'
    DESCRIPTION = 'Consume MariaDB error & slow logs and send them to GrayLog'

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

    # Consumer instance
    consumer = None
