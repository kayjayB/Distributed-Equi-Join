#!/bin/bash

# 1milLines1
# textFile1Again
# smallInput1Safe
# 500ThouLines1

cp 1milLines1.txt smallInput1.txt

cp 1milLines2.txt smallInput2.txt

cp smallInput1.txt smallInput11.txt

cp smallInput2.txt smallInput22.txt

g++ -g -O2 -Wall -D_LINUX_ equi_join.cpp -fstrict-aliasing -Wstrict-aliasing -lpthread -lrt -fpermissive -L./ -lphoenix -I./ -o equi_join

./equi_join smallInput1.txt smallInput2.txt 1 smallInput11.txt smallInput22.txt

rm smallInput1.txt
rm smallInput11.txt
rm smallInput2.txt
rm smallInput22.txt