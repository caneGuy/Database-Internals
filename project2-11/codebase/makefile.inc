## For students: change this path to the root of your code
CODEROOT = ..

#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -Wall -I$(CODEROOT) -g  -std=c++0x    # with debugging info
