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
        pFile = fopen (fileName.c_str(), "wb+");
        fclose(pFile);
        return 0;
    }
    // File exists, recreate file should fail!
    else
    {
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
    if (fileHandle.pFile)
    {
        printf("Reopen file '%s' error!\n", fileName.c_str());
        return -1;
    }
    FILE * pFile = fopen(fileName.c_str(),"rb+");
    if (pFile)
    {
        // cout << "Open " << fileName << ", ";
        fileHandle.pFile = pFile;
        return 0;
    }
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.pFile)
    {
        // cout << "Close file.\n";
        fclose (fileHandle.pFile);
        fileHandle.pFile = NULL;
        return 0;
    }
    return -1;
}


FileHandle::FileHandle()
{
    //	cout << "FileHandle constructor...\n";
    //	pFile = new FILE;
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
    //	*pFile = *fileHandle.pFile;
    appendPageCounter = fileHandle.appendPageCounter;
    writePageCounter = fileHandle.writePageCounter;
    readPageCounter = fileHandle.readPageCounter;
    return *this;
}

FileHandle::~FileHandle()
{
    pFile = NULL;
    //	delete pFile;
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
    // assume pageNum is valid
    if (data)
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
    if (data)
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
    fseek(pFile, 0, SEEK_END);
    unsigned int numberOfPages = ftell(pFile)/PAGE_SIZE;
    return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
