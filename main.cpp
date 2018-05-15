#include <string.h>
#include <iostream>
#include <vector>
#include <memory>
#include <time.h>
#include <sys/time.h>

#include "hashFunction.h"
#include "fileManager.h"

#include "mpi.h"
#include "omp.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

vector<string> join(string key, string lineFromR1, vector<string> linesFromR2)
{	
	string result;
	vector<string> results;
	
	for (auto i:linesFromR2)
	{
		size_t startOfKey = i.find(key);
		size_t endOfKey = startOfKey + key.length();
		result = lineFromR1 + i.substr(0,startOfKey) + i.substr(endOfKey+1, (i.length()-endOfKey));
		results.push_back(result);
	}
	return results;
}

vector<string> query(shared_ptr<hashFunction> hasher, string key, string lineFromR1)
{
	vector<string> queryResults = hasher->FindValue(key); 
	vector<string> results;
	
	if (!queryResults.empty())
	{
		results = join(key,lineFromR1 ,queryResults);
	}
	return results;
}

int main(int argc, char *argv[])
{
	//Bomp_set_num_threads(8);
	int numprocs, rank, namelen;
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int iam = 0, np = 1;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Get_processor_name(processor_name, &namelen);
	
	struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

	int prev_length = 0;
	int length_track = 0;

	if (rank == 0) {
		fileManager fileHandler;
		fileHandler.deleteFile("joinedFile.txt");

		ofstream myFile;
		myFile.open("joinedFile.txt", std::ios::app);

		#pragma omp critical
		for (auto i = 1; i < numprocs; i++){
			int results_length;

			MPI_Recv(&results_length, 1, MPI_INT, i, i, MPI_COMM_WORLD,MPI_STATUS_IGNORE);

			for (auto j=0; j < results_length; j++){
				int line_length;
				char char_array[1024];
				memset(char_array, 0, sizeof(char_array));
				MPI_Recv(&line_length, 1, MPI_INT, i, i, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				MPI_Recv(&char_array[0], line_length, MPI_CHAR, i, i, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				string line(char_array);
				myFile << line << endl;
			}
		}

		myFile.close();
	}
	else {
		shared_ptr<hashFunction> hasher = make_shared<hashFunction>();
		int length_file_1, length_file_2, process_length_file_2, line_count, length_file_results;
		vector<string> linesOfFile1, linesOfFile2, linesOfFile2_node, results;
		vector<vector<string>> vec_results_final;
		fileManager fileHandler;

		linesOfFile1 = fileHandler.readFile("100thou1.txt",1);
		length_file_1 = linesOfFile1.size();
		linesOfFile2 = fileHandler.readFile("100thou2.txt",1);
		length_file_2 = linesOfFile2.size();

		process_length_file_2 = (length_file_2 / (numprocs - 1.0));
		
		if (process_length_file_2 % 2 != 0) process_length_file_2 += 1;
		length_track = rank * process_length_file_2;
		prev_length = (rank-1) * process_length_file_2;

		if (rank == numprocs-1){
			if (length_track > length_file_2){
				process_length_file_2 = process_length_file_2 - (length_track - length_file_2);
				length_track = length_file_2;
			}
			else if (length_track < length_file_2){
				process_length_file_2 = process_length_file_2 + length_file_2 - length_track;
				length_track += length_file_2 - length_track;
			}
		}

		line_count = 0;

		for (auto line : linesOfFile2){
			if (line_count >= length_track) break;
			if (line_count >= prev_length && line_count < length_track){
				linesOfFile2_node.push_back(line);
			}
			line_count++;
		}

		length_file_2 = linesOfFile2_node.size();

		#pragma omp parallel default(shared) private(iam, np) 
		{
			np = omp_get_num_threads();
			iam = omp_get_thread_num();
			printf("Working on thread %d out of %d from process %d out of %d on %s\n", iam, np, rank, numprocs, processor_name);

			#pragma omp for
			for (auto i = 0; i < length_file_2; i = i + 2)
			{
				string key = linesOfFile2_node[i];
				string value = linesOfFile2_node[i+1];
				hasher -> AddItem(key,value);
			}

			vector<vector<string>> vec_results;
			
			#pragma omp for nowait schedule(static)
			for (auto i = 0; i < length_file_1; i = i + 2) { 
				vec_results.push_back(query(hasher, linesOfFile1[i], linesOfFile1[i+1]));
			}
			
			#pragma omp for schedule(static) ordered
			for(int i=0; i<omp_get_num_threads(); i++) {
				#pragma omp ordered
				vec_results_final.insert(vec_results_final.end(), vec_results.begin(), vec_results.end());
			}
		}

		for(auto && v : vec_results_final){
			results.insert(results.end(), v.begin(), v.end());
		}

		length_file_results = results.size();

		MPI_Send(&length_file_results, 1, MPI_INT, 0,rank, MPI_COMM_WORLD);

		for (auto x=0; x < length_file_results; x++){
			const char* line_ptr = results[x].c_str();
			string line = results[x].c_str();
			int line_length = line.length();
			MPI_Send(&line_length, 1, MPI_INT, 0, rank, MPI_COMM_WORLD);
			MPI_Send(line_ptr, line_length, MPI_CHAR, 0, rank, MPI_COMM_WORLD);
		}
	}

	gettimeofday(&tv2, NULL);

    printf("Time to join: ");
    printf ("%f seconds\n", (double) (tv2.tv_usec - tv1.tv_usec) / CLOCKS_PER_SEC + (double) (tv2.tv_sec - tv1.tv_sec));

	MPI_Finalize();
}