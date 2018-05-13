#ifndef HASH_H
#define HASH_H

#include <string>
#include <iostream>
#include <vector>

using namespace std;

class hashFunction
{
public:

	hashFunction();
	
	~hashFunction();
	
	int Hash(string key);
	
	void AddItem(string key, string value);
	
	int NumberOfItemsInBucket(int index);
	
	void PrintTable();
	
	void PrintItemsInBucket(int index);
	
	vector<string> FindValue(string key);
	
private:

	static const int _tableSize= 10000;
	
	struct item{
		string key;
		string value;
		item* next;
	};
	
	item* hashTable[_tableSize];

};

#endif // HASH_H
