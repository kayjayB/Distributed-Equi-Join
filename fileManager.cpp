#include "fileManager.h"

fileManager::fileManager(string fileName, string delimeter=",")
{
}

fileManager::~fileManager()
{

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