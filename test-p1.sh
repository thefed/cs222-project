#!/bin/bash

# usage: ./test-p1.sh project1

# unzip $1.zip
# if [ $? -ne 0 ]; then
#     echo "[ERROR] ZIP File Error. Please fix it!"
#     echo
#     exit 1
# fi
cd $1
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
cd codebase
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
cd rbf
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
./rbftest1
./rbftest2
./rbftest3
./rbftest4
./rbftest5
./rbftest6
./rbftest7
./rbftest8
./rbftest8b
./rbftest9
./rbftest10
./rbftest11
./rbftest12

./rbftest_ex1
./rbftest_p1
./rbftest_p1b
./rbftest_p1c
./rbftest_p2
./rbftest_p2b
./rbftest_p3
./rbftest_p4
