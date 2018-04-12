#include <string>
#include <iostream>
#include <vector>
#include <memory>
#include <time.h>
#include <sys/time.h>

#include "hashFunction.h"
#include "joinManager.h"
#include "fileManager.h"

#include "omp.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

int main()
{

	fileManager fileHandler;
	joinManager test;
		
	fileHandler.deleteFile("joinedFile.txt");
	
	struct timeval  tv1, tv2;
    gettimeofday(&tv1, NULL);

	vector<string> linesOfFile1 = fileHandler.readFile("smallInput1.txt",1);
	vector<string> linesOfFile2 = fileHandler.readFile("smallInput2.txt",1);

	#pragma omp parallel
	{
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
	
}