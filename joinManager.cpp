#include "joinManager.h"

joinManager::joinManager()
{
	hasher = make_shared<hashFunction>();
	
}

joinManager::~joinManager()
{
}

void joinManager::join(string key, string lineFromR1, vector<string> linesFromR2)
{	
	string result;
	
	if (lineFromR1.find(key)==string::npos)
	{
		string error = "No equivalent keys found";
		writeToFile(error);
		return;
	}
	
	for (auto i:linesFromR2)
	{
		size_t startOfKey = i.find_first_of(key);
		size_t endOfKey = i.find_last_of(key);
	
		result = lineFromR1 + i.substr(0,startOfKey) + i.substr(endOfKey+1, (i.length()-endOfKey));
		cout << result << endl;
		writeToFile(result);
	}
	
}

void joinManager::writeToFile(string lineToWrite)
{
	ofstream myFile;
	myFile.open("joinedFile.txt", std::ios::app);
	myFile << lineToWrite << "\n";
	myFile.close();
	
}

void joinManager::query(string key, string lineFromR1)
{
	vector<string> queryResults = hasher->FindValue(key); 
	
	if (!queryResults.empty())
	{
		join(key,lineFromR1 ,queryResults);
	}
		
}