#!/bin/bash

pip -r requirements.txt
wget https://www.percona.com/get/pt-fingerprint
chmod u+x pt-fingerprint
