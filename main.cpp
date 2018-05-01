#include <string.h>
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
	
	struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

	if (rank == 0) {
		fileManager fileHandler;
		fileHandler.deleteFile("joinedFile.txt");

		vector<string> linesOfFile1, linesOfFile2, results;

		linesOfFile1 = fileHandler.readFile("smallInput1.txt",1);
		linesOfFile2 = fileHandler.readFile("smallInput2.txt",1);

		int length_file_1 = linesOfFile1.size();
		int length_file_2 = linesOfFile2.size();

		#pragma omp critical
		for (auto i = 1; i < numprocs; i++){
			int process_length_file_2 = (length_file_2 / (numprocs - 1.0));
			if (i * process_length_file_2 > length_file_2){
				process_length_file_2 = length_file_2 - (i - 1) * process_length_file_2;
			}
			MPI_Send(&length_file_1, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
			MPI_Send(&process_length_file_2, 1, MPI_INT, i, 0, MPI_COMM_WORLD);

			for (auto x=0; x < length_file_1; x++){
				const char* line_ptr = linesOfFile1[x].c_str();
				string line = linesOfFile1[x].c_str();
				int line_length = line.length();
				MPI_Send(&line_length, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
				MPI_Send(line_ptr, line_length, MPI_CHAR, i, 0, MPI_COMM_WORLD);
			}

			for (auto y= (i-1) * process_length_file_2; y < i * process_length_file_2; y++){
				const char* line_ptr = linesOfFile2[y].c_str();
				string line = linesOfFile2[y].c_str();
				int line_length = line.length();
				MPI_Send(&line_length, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
				MPI_Send(line_ptr, line_length, MPI_CHAR, i, 0, MPI_COMM_WORLD);
			}

			int results_length;

			MPI_Recv(&results_length, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,MPI_STATUS_IGNORE);

			for (auto i=0; i < results_length; i++){
				int line_length;
				char char_array[1024];
				memset(char_array, 0, sizeof(char_array));
				MPI_Recv(&line_length, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				MPI_Recv(&char_array[0], line_length, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				string line(char_array);
				results.push_back(line);
			}
		}

		ofstream myFile;
		myFile.open("joinedFile.txt", std::ios::app);

		#pragma omp single
		for (auto i=results.begin(); i != results.end(); i++){
			myFile << *i << "\n";
		}
		myFile.close();
	}
	else {
		int length_file_1, length_file_2;
		vector<string> linesOfFile1, linesOfFile2, results;
		vector<vector<string>> buffer;
		joinManager test;

		MPI_Recv(&length_file_1, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&length_file_2, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);

		cout << "File 2 length: " << length_file_2 << endl;

		#pragma omp single
		for (auto i=0; i < length_file_1; i++){
			int line_length;
			char char_array[1024];
			memset(char_array, 0, sizeof(char_array));
			MPI_Recv(&line_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Recv(&char_array[0], line_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			string line(char_array);
			linesOfFile1.push_back(line);
		}

		#pragma omp single
		for (auto i=0; i < length_file_2; i++){
			int line_length;
			char char_array[1024];
			memset(char_array, 0, sizeof(char_array));
			MPI_Recv(&line_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Recv(&char_array[0], line_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			string line(char_array);
			linesOfFile2.push_back(line);
		}

		#pragma omp parallel default(shared) private(iam, np) 
		{
			np = omp_get_num_threads();
			iam = omp_get_thread_num();
			printf("Working on thread %d out of %d from process %d out of %d on %s\n", iam, np, rank, numprocs, processor_name);

			#pragma omp for
			for (unsigned int i=0; i< linesOfFile1.size(); i = i+2)
			{
				//int tid = omp_get_thread_num();  
				//printf("Hello World from thread = %d\n", tid);  
				string key = linesOfFile1[i];
				string value = linesOfFile1[i+1];
				test.hasher -> AddItem(key,value);
			}
			
			#pragma omp single
			for (unsigned int i=0; i<linesOfFile2.size();i = i+2)
			{
			//	int tid = omp_get_thread_num();  
			//	printf("Hello World from thread = %d\n", tid); 
				test.query(linesOfFile2[i], linesOfFile2[i+1]);
			}
		}

		results = test.getResults();

		int length_file_results = results.size();
		MPI_Send(&length_file_results, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

		#pragma omp single
		for (auto x=0; x < length_file_results; x++){
			const char* line_ptr = results[x].c_str();
			string line = results[x].c_str();
			int line_length = line.length();
			MPI_Send(&line_length, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			MPI_Send(line_ptr, line_length, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
		}
	}

	gettimeofday(&tv2, NULL);

    printf("Time to join: ");
    printf ("%f seconds\n", (double) (tv2.tv_usec - tv1.tv_usec) / CLOCKS_PER_SEC + (double) (tv2.tv_sec - tv1.tv_sec));

    printf("\n");

	MPI_Finalize();
}