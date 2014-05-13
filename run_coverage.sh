#!/bin/sh
rm ./coverage/*.html ./coverage/*.js ./coverage/*.css ./coverage/keybd_*.png
nosetests-2.7 -v --with-coverage --cover-erase --cover-html --cover-package=chorde --cover-html-dir=./coverage tests/*.py

