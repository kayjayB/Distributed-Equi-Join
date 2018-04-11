#include "hashFunction.h"

hashFunction::hashFunction()
{
	
	for (int i=0; i < _tableSize; i++)
	{
		hashTable[i] = new item;
		hashTable[i] -> key = "empty";
		hashTable[i] -> value = "empty";
		hashTable[i] -> next = NULL;
		
	}
}

int hashFunction::Hash(string key)
{
	int hashVal=0;
	
	for (int i=0; i < key.length(); i++)
	{
		hashVal += (int)key[i];
	}
	
	int index;
	index = hashVal % _tableSize;
	cout << index<< endl;
	return index;
}

void hashFunction::AddItem(string key, string value)
{
	int index = Hash(key);
	
	if (hashTable[index] -> key == "empty")
	{
		hashTable[index] -> key = key;
		hashTable[index] -> value = value;
	}
	else
	{
		item* Ptr = hashTable[index];
		item* n = new item;
		n -> key = key;
		n -> value = value;
		n -> next = NULL;
		while (Ptr -> next != NULL)
		{
			Ptr = Ptr -> next;
		}
		Ptr -> next = n;
	}
}

int hashFunction::NumberOfItemsInBucket(int index)
{
	int count = 0;
	
	if (hashTable[index] -> key == "empty")
	{
		return count;
	}
	else 
	{
		count++;
		item* Ptr = hashTable[index];
		while (Ptr -> next != NULL)
		{
			count++;
			Ptr = Ptr -> next;
		}
	}
	return count;
}

hashFunction::~hashFunction()
{
}

void hashFunction::PrintTable()
{
	int number;
	
	for (int i=0; i< _tableSize; i++)
	{
		number = NumberOfItemsInBucket(i);
		cout << "..........................." << endl;
		cout << " index = " << i << endl;
//		cout << hashTable[i]->key << endl;
//		cout << hashTable[i]->value << endl;
		cout << "Number of items in bucket " << number << endl;
		PrintItemsInBucket(i);
		cout << "..........................." << endl;
		
	}
}

void hashFunction::PrintItemsInBucket(int index)
{
	item* Ptr = hashTable[index];
	
	if (Ptr -> key == "empty")
	{
		cout << " index " << index << " is empty " << endl;
	}
	else 
	{
		cout << " index " << index << " contains: " << endl;

		while (Ptr != NULL)
		{
			cout << "..........................." << endl;
			cout << Ptr->key << endl;
			cout << Ptr->value << endl;
			cout << "..........................." << endl;
			Ptr = Ptr -> next;
		}
	}
}

vector<string> hashFunction::FindValue(string key)
{
	int index = Hash(key);
	bool found = false;
	string value;
	
	vector <string> values;
	
	item* Ptr = hashTable[index];
	
	while (Ptr != NULL)
	{
		if (Ptr->key == key)
		{
			found = true;
			value = Ptr-> value;
			string temp = key + "," + value;
			values.push_back(temp);
		}
		Ptr = Ptr->next;
	}
	
	return values;
}