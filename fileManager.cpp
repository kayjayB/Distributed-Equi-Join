#include "fileManager.h"

fileManager::fileManager(string fileName, string key, string delimeter=",")
{
    _file_name = fileName;
    if (delimeter != ","){
        _delimeter = delimeter;
    }
}

fileManager::~fileManager()
{

}

string fileManager::readFile()
{
	ifstream file(_file_name); // pass file name as argment
	string linebuffer;

	while (file && getline(file, linebuffer)){
		if (linebuffer.length() == 0)continue;
		
	}
}

vector<string> fileManager::split(char *phrase, string delimiter)
{
	
    vector<string> list;
    string s = string(phrase);
    size_t pos = 0;
    string token;

    while ((pos = s.find(delimiter)) != string::npos) 
	{
        token = s.substr(0, pos);
        list.push_back(token);
        s.erase(0, pos + delimiter.length());
    }

    return list;

}