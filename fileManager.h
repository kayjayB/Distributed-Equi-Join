#ifndef FILEMANAGER_H
#define FILEMANAGER_H

class fileManager
{
public:
	fileManager(string fileName, string delimeter);
	~fileManager();

	vector<string> split(char *phrase, string delimiter);
};

#endif // FILEMANAGER_H
