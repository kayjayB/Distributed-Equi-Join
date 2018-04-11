#ifndef FILEMANAGER_H
#define FILEMANAGER_H

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

class fileManager
{
public:
	fileManager(string fileName, string key, string delimeter);
	~fileManager();

	string readFile();
	vector<string> split(char *phrase, string delimiter);

private:
	string _file_name;
	string _delimeter;
	string _key;
	vector<string> _data;
};

#endif // FILEMANAGER_H
