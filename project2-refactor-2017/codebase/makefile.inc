## For students: change this path to the root of your code if this doesn't work
CODEROOT = ..

CC = g++

CXX = $(CC)

#CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
#CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++0x # with debugging info and the C++11 feature 
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++0x -DDEBUG # with debug prints
