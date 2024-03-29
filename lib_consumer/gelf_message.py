#!/usr/bin/env python3


"""
    Contains GELF_message, which represents a GELF message.
"""


class GELF_Message:
    """ A GELF message that supports these operations:
        * Creation, with standard and custom attributes;
        * Append a string to an existing attribute;
        * Get as string.
    """

    import socket

    ##  Variables
    ##  =========

    #: GELF message in string form.
    _message: dict[str, str] = { }
    #: Debug flags for additional output.
    debug: dict[str, bool] = { }


    ##  Constants
    ##  =========

    _CUSTOM_FIELD_PREFIX = '_'


    ##  Methods
    ##  =======

    def _get_level(self, level):
        """ Given a string that represents a severity level, return the corresponding GELF level.
            If the level is not recognised, return 'UNKNOWN'.
        """
        if level == 'ERROR':
            return '3'
        elif level == 'WARNING':
            return '4'
        elif level == 'NOTE':
            return '6'
        else:
            return 'UNKNOWN'

    def create_field(self, is_custom, key, value):
        """ Compose a single key/value couple in a GELF line.
            The field must not exist, or a KeyError exception will be returned.
        """
        if is_custom:
            key = self._CUSTOM_FIELD_PREFIX + key
        self._message[key] = value

    def append_to_field(self, is_custom, key, value):
        """ Append a string to the specified field value.
            key can be a standard GELF field of a custom field without
            the initial underscore.
            The key must exist, or a KeyError exception will be returned.
            value is the value to append.
        """
        if is_custom:
            key = self._CUSTOM_FIELD_PREFIX + key

        if key in self._message:
            self._message[key] = self._message[key] + "\n" + str(value)
        else:
            raise KeyError('Invalid GELF field: "' + str(key) + '"')

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

        self.create_field(False, 'version', version)
        # The hostname was set previously
        self.create_field(False, 'host', host)
        # 'MariaDB Error Log' or 'MariaDB Slow Log'
        self.create_field(False, 'short_message', short_message)
        self.create_field(False, 'timestamp', str(timestamp))
        # Same levels as syslog:
        # 0=Emergency, 1=Alert, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Informational, 7=Debug
        # https://docs.delphix.com/docs534/system-administration/system-monitoring/setting-syslog-preferences/severity-levels-for-syslog-messages
        self.create_field(False, 'level', self._get_level(level))

        # all custom fields (not mentioned in GELF specs)
        # must start with a '_'
        for key in extra:
            self.create_field(True, key, extra[key])

    def to_string(self):
        """ Return the GELF message as string. """
        gelf_message = '{'

        is_first = True
        for key in self._message:
            if not is_first:
                gelf_message = gelf_message + ','
            is_first = False
            gelf_message = gelf_message + '"' + key + '":"' + self._message[key].replace('"', '\\"') + '"'

        gelf_message = gelf_message + '}'

        return gelf_message


    ## DEBUG METHODS
    ## =============

    def attribute_exists(self, key):
        """ Return whether the GELF message contains the specified key. """
        return key in self._message

    def get_attribute_by_name(self, key, defaultValue = None):
        """ Return the specified key or None. """
        return self._message.get(key, defaultValue)

    def get_attribute_by_value(self, needle):
        """ Return the list of attributes with the given value. """
        key_list = [ ]
        for key in self._message:
            current_value = self._message[key]
            if current_value == needle:
                key_list.append(current_value)
        return key_list

    def get_attribute_count(self):
        """ Return the number of attributes in the GELF message. """
        return len(self._message)

#EOF