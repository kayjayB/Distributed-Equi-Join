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
	fileManager fileHandler("inputFile.txt",2);
	joinManager test;

	test.hasher->AddItem("KJ", "23");
	test.hasher->AddItem("Lara", "Potot");
	test.hasher->AddItem("KJ", "230");
	test.hasher->AddItem("Lara", "230");
	test.hasher->AddItem("Jared", "293");
	test.hasher->AddItem("Jared", "2254");
	test.hasher->AddItem("Matt", "12345");
	test.hasher->AddItem("Matt", "98765");
	test.hasher->AddItem("KJ","2349");
	test.hasher->AddItem("Lara","Hello");
	test.hasher->AddItem("Lara","I'm");
	test.hasher->AddItem("KJ","395678r3e20w1p90");
	
	test.hasher->PrintTable();
	
	string test1 = "Lara,23,968";
	
	string key = "KJ";
	
	test.query(key, test1);
	
}