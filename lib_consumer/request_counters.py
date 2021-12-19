#!/usr/bin/env python


""" When an action is requested, for example via a signal or a TCP
    communication, a program may not be able to accomplish immediately.
    This class allows to record a request to perform the action later.
    For each action we record a counter of received requests, in case
    the program has a use for it.
    After an action is performed, the program is supposed to reset
    the counter.
"""


class Request_Counters:
    """
        A dictionary of request counters, with the methods necessary to
        increment, read or reset the counter.
    """

    ##  Variables
    ##  =========

    #: Dictionary of request counters.
    _request_counters = { }


    ##  Methods
    ##  =======

    def __init__(self, action_list):
        for action in action_list:
            self._request_counters[action] = 0

    def increment(self, action):
        try:
            self._request_counters['action'] = self._request_counters['action'] + 1
        except KeyError as e:
            raise Exception('Action does not exist: \'' + action + '\'')

    def reset(self, action):
        try:
            self._request_counters['action'] = 0
        except KeyError as e:
            raise Exception('Action does not exist: \'' + action + '\'')

    def get_action(self, action):
        try:
            return self._request_counters['action']
        except KeyError as e:
            raise Exception('Action does not exist: \'' + action + '\'')

    def was_requested(self, action):
        try:
            return (self._request_counters['action'] > 0)
        except KeyError as e:
            raise Exception('Action does not exist: \'' + action + '\'')

#EOF