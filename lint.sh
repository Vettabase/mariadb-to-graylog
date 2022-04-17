#!/bin/bash


# let's not use /dev/null because this script may not run as root
which mypy > tmp
r=$?
rm -f tmp

if [ $r == '0' ];
then
    echo 'OK: mypy is installed'
else
    echo 'Installing mypy...'
    python3 -m pip install mypy
    if [ $? == '0' ];
    then
        echo 'OK: mypy successfully installed'
    else
        echo 'ERROR: Could not install mypy'
        exit 1
    fi
    python3 -m pip install types-requests
    mypy mariadb-log-consumer.py > tmp
    rm -f tmp
    yes | mypy src --install-types || true
fi

echo
mypy mariadb-log-consumer.py
