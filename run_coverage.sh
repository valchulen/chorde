#!/bin/sh
set -e
rm -f ./coverage/*.html ./coverage/*.js ./coverage/*.css ./coverage/keybd_*.png ./coverage/status.dat
rm -f .coverage
coverage-2.7 run --branch setup.py test $@
coverage-2.7 report $(find lib/chorde -name '*.py')
coverage-2.7 html -d ./coverage $(find lib/chorde -name '*.py')
coverage-2.7 xml -o coverage.xml $(find lib/chorde -name '*.py')

