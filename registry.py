#!/usr/bin/env python


""" This module only contains a Registry class, which contains global (program-level)
    constants and variables.
"""


class Registry:
    """
    Global constants (Registry pattern)
    """

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

    # Consumer instance
    consumer = None
