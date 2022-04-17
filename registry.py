#!/usr/bin/env python3


""" This module only contains a Registry class, which contains global (program-level)
    constants and variables.
"""


from typing import Optional
from typing import Any


class Registry:
    """
    Global constants and variables (Registry pattern)
    """


    ##  Global Constants
    ##  ================

    #: Program name
    PROGRAM = 'MariaDB To Graylog'
    #: Program version
    VERSION = '0.1'
    #: Program description
    DESCRIPTION = 'Consume MariaDB error & slow logs and send them to GrayLog'

    #: Length of the "short" field in characters, in GELF messages.
    #: Does not include the event severity.
    SHORT_MESSAGE_LENGTH = 20

    #: Backup flags, for additional output
    DEBUG = {
        # Normally, the consuming loop handles exceptions, to prevent program
        # crashes from leaving a lock file. Set to True to dodge exceptions
        # instead, to get a more meaningful traceback.
        'DODGE_EXCEPTIONS': True,
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
    consumer = None        # type: Optional[Any]

#EOF