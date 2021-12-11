#!/usr/bin/env python


"""
    A GELF message that can be created and sent.
"""


class GELF_message:
    ##  Variables
    ##  =========

    #: GELF message in string form.
    message = None
    #: Debug flags for additional output.
    debug = None


    ##  Methods
    ##  =======

    def get_field(self, key, value):
        """ Compose a single key/value couple in a GELF line"""
        value = value.replace('\\', '\\\\')
        return '"' + key + '":"' + value + '"'

    def __init__(
            self,
            debug,
            version,
            # Mandatory GELF properties
            timestamp,
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

        self.debug = debug

        message = '{'

        message += self.get_field('version', version)
        # The hostname was set previously
        message += ',' + self.get_field('host', host)
        # 'MariaDB Error Log' or 'MariaDB Slow Log'
        message += ',' + self.get_field('short_message', short_message)
        message += ',' + self.get_field('timestamp', str(timestamp))
        # Same levels as syslog:
        # 0=Emergency, 1=Alert, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Informational, 7=Debug
        # https://docs.delphix.com/docs534/system-administration/system-monitoring/setting-syslog-preferences/severity-levels-for-syslog-messages
        message += ',' + self.get_field('level', level)

        # all custom fields (not mentioned in GELF specs)
        # must start with a '_'
        for key in extra:
            message += ',' + self.get_field('_' + key, extra[key])

        message += '}'

        self.message = message

    def send(self):
        """ Send the GELF message. """
        if self.debug['GELF_MESSAGES']:
            print(self.message)
        return True

#EOF