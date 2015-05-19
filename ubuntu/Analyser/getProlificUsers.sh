#!/bin/sh
nohup mpirun -n 3 python ./Python/getProlificUsers.py -a "http://144.6.227.96:5984" -n perth -v "_design/groupview/_view/groupview" -f token_key/ -r ./Result > getProlificUsers.out&
