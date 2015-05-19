#!/bin/sh
nohup mpirun -n 4 --hostfile ./hostfile python mpi_crawler.py -a "http://144.6.227.96:5984" -n "perth" -f ./token_key > mpi_crawler.out & 
