# mariadb-to-graylog

Consume MariaDB error log and slow log, and send events to Graylog.


## Install

To install the dependencies and download pt-fingerprint:

```
./install.sh
```


## Usage

TODO: Add explanation. The help message here is a placeholder.


```
  -h, --help            show this help message and exit
  -t LOG_TYPE, --log-type LOG_TYPE
                        Type of log to consume. Permitted values: error, slow.
                        Permitted aliases: errorlog, errorlog. Case-insensitive.
  -l LOG, --log LOG     Path and name of the log file to consume.
  --limit LIMIT         Maximum number of sourcelog entries to process. Zero or
                        a negative value means process all sourcelog entries.
                        Implies --stop-never.
  --offset OFFSET       Number of sourcelog entries to skip at the beginning.
                        Zero or a negative value means skip nothing.
  --stop STOP           When the program must stop. Allowed values:
                            eof:    When the end of file is reached.
                            limit:  When --limit sourcelog entries are processed.
                            never:  Always keep running, waiting for new
                                    entries to process.
  --eof-wait EOF_WAIT   Number of milliseconds to wait after reaching the sourcelog
                        end, before checking if there are new contents.
  --message-wait MESSAGE_WAIT
                        Number of milliseconds to wait before processing the
                        next message, as a trivial mechanism to avoid overloading
                        the server.
  --label LABEL         ID for the program execution. To calls with different
                        IDs are allowed to run simultaneously.
                        Default: same value as --log-type.
  -f, --force-run       Don't check if another instance of the program is
                        running, and don't prevent other instances from running.
  -H GRAYLOG_HOST, --graylog-host GRAYLOG_HOST
                        Graylog hostname.
  --graylog-port-udp GRAYLOG_PORT_UDP
                        Graylog UDP port.
  --graylog-port-tcp GRAYLOG_PORT_TCP
                        Graylog TCP port.
  --graylog-port-http GRAYLOG_PORT_HTTP
                        Graylog HTTP port.
  --graylog-http-timeout-idle GRAYLOG_HTTP_TIMEOUT_IDLE
                        Timeout for the HTTP call when no data is received.
  --graylog-http-timeout GRAYLOG_HTTP_TIMEOUT
                        Timeout for the HTTP call. This is a hard limit.
  -n HOSTNAME, --hostname HOSTNAME
                        Hostname as it will be sent to Graylog.
  -T, --truncate-eventlog
                        Truncate the eventlog before starting. Useful if the
                        sourcelog was replaced.
```


### Help

More info will follow. In the meanwhile, for usage refer to built-in help:

```
/path/to/mariadb-log-consumer.py --help
/path/to/mariadb-log-consumer.py -h
```


### Signals

**Do not terminate the script with SIGTERM!**

The script maintains eventlogs to remember which entries from the source logs were sent.
If it stops and restarts, it will be able to resume consuming the source log from
the right point.

The script is smart enough to avoid stopping before writing an update to the eventlog.
So, **SIGTERM** and **SIGINT** can safely be used. But **SIGTERM** cannot be handled by
a program, so it is not safe by nature.

Eventlogs are rotated by sending a **SIGHUP** to the script.


### Error handling

The program tries to always terminate with a meaningful exit code:

- 0 - Success
- 1 - Generic error
- 2 - Invalid parameters
- 3 - External error (OS, hardware, network...)

A known problem is that unexpected errors are not handled, and fail in the standard Python way.
When this happens, the Lock File is likely to exist and not be deleted. The lock file is meant
to prevent the script from having multiple simoultaneous instances that read the same Source Log
(or have the same `--label`).

When this happens, trying to restart the script will produce an error like this:

```
Lock file exists or cannot be created: /psth/to/lock-file
```

To fix the problem, just delete the Lock File.


## Testing with Netcat

To test the consumer, you may want to use netcat.

For example, to test it with the HTTP protocol, first launch Netcat in a loop:

```
while true ; do nc -l 12201 ; done
```

Now make sure that you have a log that can be consumed. For example, copy an error log
to `logs/error.log`.

Then, run the script telling it to send GELF messages to localhost over HTTP:

```
./mariadb-log-consumer.py --log-type=error --log=logs/error.log --graylog-host=localhost --graylog-port-http=12201 -T
```


## Copyright and License

Copyright  2021 2022  Vettabase Ltd

License: BSD 3 (BSD-New).

Developed and maintained by Vettabase Ltd:

https://vettabase.com

Contributions are welcome.
