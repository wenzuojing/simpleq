#! /bin/sh

buildoutput=.build

if [ ! -d $buildoutput ]; then
    mkdir simpleq
else
    rm -rf $buildoutput
fi

mv simpleq.conf ./$buildoutput