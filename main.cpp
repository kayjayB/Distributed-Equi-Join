#include <string>
#include <iostream>
#include <vector>
#include <memory>
#include <time.h>
#include <sys/time.h>

#include "hashFunction.h"
#include "joinManager.h"
#include "fileManager.h"

#include "mpi.h"
#include "omp.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

int main(int argc, char *argv[])
{
	int numprocs, rank, namelen;
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int iam = 0, np = 1;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Get_processor_name(processor_name, &namelen);

	fileManager fileHandler;
	joinManager test;
		
	fileHandler.deleteFile("joinedFile.txt");
	
	struct timeval  tv1, tv2;
    gettimeofday(&tv1, NULL);

	vector<string> linesOfFile1 = fileHandler.readFile("smallInput1.txt",1);
	vector<string> linesOfFile2 = fileHandler.readFile("smallInput2.txt",1);

	#pragma omp parallel default(shared) private(iam, np)
	{
		np = omp_get_num_threads();
    	iam = omp_get_thread_num();
		printf("Working on thread %d out of %d from process %d out of %d on %s\n",
           iam, np, rank, numprocs, processor_name);

		#pragma omp for
		for (int i=0; i< linesOfFile1.size(); i = i+2)
		{
			//int tid = omp_get_thread_num();  
			//printf("Hello World from thread = %d\n", tid);  
			string key = linesOfFile1[i];
			string value = linesOfFile1[i+1];
			test.hasher -> AddItem(key,value);
		}
		#pragma omp for
		for (int i=0; i<linesOfFile2.size();i = i+2)
		{
		//	int tid = omp_get_thread_num();  
		//	printf("Hello World from thread = %d\n", tid);  
			test.query(linesOfFile2[i], linesOfFile2[i+1]);
		}
	}

	gettimeofday(&tv2, NULL);

    printf("Time to join: ");
    printf ("%f seconds\n", (double) (tv2.tv_usec - tv1.tv_usec) / CLOCKS_PER_SEC + (double) (tv2.tv_sec - tv1.tv_sec));

    printf("\n");

	MPI_Finalize();
	
}