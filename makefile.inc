## For students: change this path to the root of your code if this doesn't work
CODEROOT = ..
## CODEROOT = ~/Desktop/cs222-fall19-team-19

#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11  # with debugging info and the C++11 feature
