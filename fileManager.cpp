#include "fileManager.h"

fileManager::fileManager()
{
}

fileManager::~fileManager()
{
}

vector<string> fileManager::readFile(string fileName, int column, char delimeter)
{
	ifstream file(fileName); // pass file name as argment
	string linebuffer;
	string key;
	vector<string> data;

	while (file && getline(file, linebuffer)){
		if (linebuffer.length() == 0) continue;
		vector<string> line = split(linebuffer, delimeter);
		key = line.at(column);
		data.push_back(key);
		data.push_back(linebuffer);
	}
	
	return data;
}

vector<string> fileManager::split(string line, char delimiter)
{
	stringstream ss(line);
	string item;
	vector<string> list;
	
	while (getline(ss, item, delimiter))
	{
		list.push_back(item);
	}
	
	return list;
}

void fileManager::deleteFile(const char * fileName)
{
	if( remove( fileName ) != 0 )
		cout << "Error removing file" << endl;
	else
		cout << "File removed" << endl;
}