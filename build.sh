#!/bin/sh

BUILD_SH=../uctrac/deploy/build.py
GIT_HOME=`pwd`

if [ ! -d deploy ]; then ln -s ../uctrac/deploy deploy ; fi

$BUILD_SH --gitdir=$GIT_HOME --moduledir=$GIT_HOME/modules --modules=$1

