#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

using namespace std;

class fileManager
{
public:
	fileManager(string fileName, int column, char delimeter = '|');
	~fileManager();

	vector<string> readFile();
	vector<string> split(string line, char delimiter);

private:
	string _file_name;
	int _column;
	char _delimeter;
};

#endif // FILEMANAGER_H
