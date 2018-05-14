mpi-proj:
	mpic++ --std=gnu++14 -fopenmp -lgomp -lm -Wall -g main.cpp fileManager.cpp hashFunction.cpp -o Project