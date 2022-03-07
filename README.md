# mariadb-to-graylog

Consume MariaDB error log and slow log, and send events to Graylog.

to install the dependencies:

```
pip -r requirements.txt
```

More info will follow. In the meanwhile, for usage refer to built-in help:

```
/path/to/mariadb-log-consumer.py --help
/path/to/mariadb-log-consumer.py -h
```


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

Copyright  2021  Vettabase Ltd

License: BSD 3 (BSD-New).

Developed and maintained by Vettabase Ltd:

https://vettabase.com

Contributions are welcome.
