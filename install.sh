#!/bin/bash

pip -r requirements.txt
wget https://www.percona.com/get/pt-fingerprint
chmod ugo+x pt-fingerprint
