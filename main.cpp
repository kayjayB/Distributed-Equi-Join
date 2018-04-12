#include <string>
#include <iostream>
#include <vector>
#include <memory>

#include "hashFunction.h"
#include "joinManager.h"
#include "fileManager.h"

using namespace std;

using std::shared_ptr;
using std::make_shared;

int main()
{
	fileManager fileHandler("smallInput1.txt",2);
	joinManager test;
	
	vector<string> linesOfFile1 = fileHandler.readFile();
	
	for (int i=0; i< linesOfFile1.size(); i+2)
	{
		string key = linesOfFile1[i];
		string value = linesOfFile1[i+1];
		test.hasher -> AddItem(key,value);
	}

//	test.hasher->AddItem("KJ", "23");
//	test.hasher->AddItem("Lara", "Potot");
//	test.hasher->AddItem("KJ", "230");
//	test.hasher->AddItem("Jared", "2254");
//	test.hasher->AddItem("Matt", "12345");
//	test.hasher->AddItem("Matt", "98765");
//	test.hasher->AddItem("KJ","2349");
//	test.hasher->AddItem("Lara","Hello");
//	test.hasher->AddItem("Lara","I'm");
//	test.hasher->AddItem("KJ","395678r3e20w1p90");
//	
//	test.hasher->PrintTable();
//	
//	string test1 = "Lara,23,968";
//	
//	string key = "KJ";
//	vector<string> linesOfFile1;
//	vector<string> keys;
//	
//	linesOfFile1.push_back("NewName, lemons, 90");
//	linesOfFile1.push_back("KJ, coffee, 68");
//	linesOfFile1.push_back("Lara, potatoes, 21");
//	linesOfFile1.push_back("Jared, nandos, 67");
//	linesOfFile1.push_back("Matt, sugar, 00");
//	
//	keys.push_back("NewName");
//	keys.push_back("KJ");
//	keys.push_back("Lara");
//	keys.push_back("Jared");
//	keys.push_back("Matt");
	
	
//	vector<string> linesOfFile2 = fileHandler.readFile();
//	
//	for (int i=0; i<linesOfFile2.size();i+2)
//	{
//		cout << "key is: " << linesOfFile2[i] << endl;
//		test.query(linesOfFile2[i], linesOfFile2[i+1]);
//	}
	

//	test.query(key, test1);
}