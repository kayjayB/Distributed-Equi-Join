#include <string>
#include <iostream>
#include <vector>
#include <memory>

#include "hashFunction.h"
#include "joinManager.h"
#include "fileManager.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

int main()
{

	fileManager fileHandler;
	joinManager test;
		
	fileHandler.deleteFile("joinedFile.txt");
	
	vector<string> linesOfFile1 = fileHandler.readFile("smallInput2.txt",1);
	
	for (int i=0; i< linesOfFile1.size(); i = i+2)
	{
		string key = linesOfFile1[i];
		string value = linesOfFile1[i+1];
		test.hasher -> AddItem(key,value);
	}
	
	//fileManager fileHandler2("smallInput2.txt",1);
	vector<string> linesOfFile2 = fileHandler.readFile("smallInput2.txt",1);
	
	for (int i=0; i<linesOfFile2.size();i = i+2)
	{
		//cout << "key is: " << linesOfFile2[i] << endl;
		test.query(linesOfFile2[i], linesOfFile2[i+1]);
	}
	
}