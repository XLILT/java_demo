#!/usr/bin/env bash

################################################
# File Name: init.sh
# Author: mxl
# Mail: xiaolongicx@gmail.com
# Created Time: 2018-11-30 15:23
################################################

javac A.java

mkdir build
cd build
cmake ..
make

cp ../A.class .
./main

