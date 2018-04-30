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
	
	for (auto i:linesFromR2)
	{
//		cout << "OG line from file1: " << i << endl;
		size_t startOfKey = i.find(key);
//		cout << "Start of key is: " << startOfKey << endl;
		size_t endOfKey = startOfKey + key.length();
//		cout << "End of key is: " << endOfKey << endl;
//		cout << "line from file2: " << lineFromR1 << endl;
//		cout << "line from file1: " << i.substr(0,startOfKey) + i.substr(endOfKey+1, (i.length()-endOfKey)) << endl;
		result = lineFromR1 + i.substr(0,startOfKey) + i.substr(endOfKey+1, (i.length()-endOfKey));
//		cout << "line to be joined is: " << result << endl;
		results.push_back(result);
	}
	
}

vector<string> joinManager::query(string key, string lineFromR1)
{
	vector<string> queryResults = hasher->FindValue(key); 
	
	if (!queryResults.empty())
	{
		join(key,lineFromR1 ,queryResults);
	}
	return results;
}