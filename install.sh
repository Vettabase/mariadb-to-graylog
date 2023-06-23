#!/bin/bash


# Install required Python modules
pip -r requirements.txt

# Download (replace) pt-fingerprint
rm -f pt-fingerprint
wget https://www.percona.com/get/pt-fingerprint
chmod ugo+x pt-fingerprint
