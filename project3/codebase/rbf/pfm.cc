#include "pfm.h"

extern ofstream fcout;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	FILE *pFile;
	if (fopen (fileName.c_str(), "r") == NULL)
	{
//		cout << "Create " << fileName << "...\n";
		pFile = fopen (fileName.c_str(), "wb+");
		fclose(pFile);
		return 0;
	}
	else
	{
		// File exists, create file again should fail!
		return -1;
	}
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	remove (fileName.c_str());
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (fileHandle.pFile != NULL)
	{
		cout << "File [" << fileName << "] cannot be reopened!\n";
		return -1; // File cannot be reopened!
	}
	FILE * pFile = fopen(fileName.c_str(),"rb+");
	if (pFile!=NULL)
	{
//		cout << "Open " << fileName << ", ";
		fileHandle.pFile = pFile;
		return 0;
	}
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (fileHandle.pFile!=NULL)
	{
//		cout << "Close file.\n";
		fclose (fileHandle.pFile);
		fileHandle.pFile = NULL;
		return 0;
	}
    return -1;
}


FileHandle::FileHandle()
{
//	cout << "FileHandle constructor...\n";
	pFile = NULL;
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

FileHandle::FileHandle(const FileHandle &fileHandle) // Copy constructor
{
	pFile = fileHandle.pFile;
	readPageCounter = fileHandle.readPageCounter;
	writePageCounter = fileHandle.writePageCounter;
	appendPageCounter = fileHandle.appendPageCounter;
	cout << "FileHandle copy constructor...\n";
}

FileHandle& FileHandle::operator=(const FileHandle &fileHandle) // Assignmenent function
{
//	cout << "FileHandle assign function called...\n";
	pFile = fileHandle.pFile;
	appendPageCounter = fileHandle.appendPageCounter;
	writePageCounter = fileHandle.writePageCounter;
	readPageCounter = fileHandle.readPageCounter;
	return *this;
}

FileHandle::~FileHandle()
{
	pFile = NULL;
//	cout << "FileHandle destructor...\n";
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	fseek(pFile, pageNum*PAGE_SIZE, SEEK_SET); // SEEK_SET is the beginning of file (origin)
	fread(data, 1, PAGE_SIZE, pFile);
	readPageCounter = readPageCounter + 1;
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	// Assume pageNum is valid
	if (data != NULL)
	{
		fseek(pFile, pageNum*PAGE_SIZE, SEEK_SET); // offset, origion
		fwrite(data, 1, PAGE_SIZE, pFile);
		writePageCounter = writePageCounter + 1;
		return 0;
	}
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
	if (data != NULL)
	{
		int numberOfPages = getNumberOfPages();
		fseek(this->pFile, numberOfPages*PAGE_SIZE, SEEK_SET);
		fwrite (data, 1, PAGE_SIZE, this->pFile);
		appendPageCounter = appendPageCounter + 1;
		return 0;
	}
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
	unsigned int numberOfPages;
	fseek(pFile, 0, SEEK_END);
	numberOfPages = ftell(pFile)/PAGE_SIZE;
	return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}
