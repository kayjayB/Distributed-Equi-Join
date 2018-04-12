#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <memory>

using namespace std;

class fileManager
{
public:
	fileManager();
	~fileManager();

	vector<string> readFile(string fileName, int column, char delimeter = '|');
	vector<string> split(string line, char delimiter);
	void deleteFile(const char * fileName);

private:

};

#endif // FILEMANAGER_H
