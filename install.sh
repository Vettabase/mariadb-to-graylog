#!/bin/bash

pip -r requirements.txt

rm -f pt-fingerprint
wget https://www.percona.com/get/pt-fingerprint
chmod ugo+x pt-fingerprint
