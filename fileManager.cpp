#include "fileManager.h"

fileManager::fileManager(string fileName, int column, char delimeter)
{
    _file_name = fileName;
	_column = column;
    if (delimeter != '|'){
        _delimeter = delimeter;
    }
}

fileManager::~fileManager()
{
}

vector<string> fileManager::readFile()
{
	ifstream file(_file_name); // pass file name as argment
	string linebuffer;
	string key;
	vector<string> data;

	while (file && getline(file, linebuffer)){
		if (linebuffer.length() == 0) continue;
		cout << "line buffer: " << linebuffer << endl;
		vector<string> line = split(linebuffer, _delimeter);
		key = line.at(_column);
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
		cout << "item is: " << item << endl;
		list.push_back(item);
	}
	
	return list;
}