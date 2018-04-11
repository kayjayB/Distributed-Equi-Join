#ifndef JOINMANAGER_H
#define JOINMANAGER_H

#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <memory>

#include "hashFunction.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

class joinManager
{
public:
	joinManager();
	~joinManager();
	
	shared_ptr<hashFunction> hasher;
	
	void join(string key, string lineFromR1, vector<string> linesFromR2);
	void query(string key, string lineFromR1);

private:	
	void writeToFile(string lineToWrite);

};

#endif // JOINMANAGER_H
