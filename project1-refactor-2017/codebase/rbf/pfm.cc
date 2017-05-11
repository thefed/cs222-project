#include "pfm.h"
#include<stdio.h>
#include<string>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager() {}

PagedFileManager::~PagedFileManager() {}

// check if file exists
bool fileExist(const string &fileName) {
    if (FILE *file = fopen(fileName.c_str(), "r")) {
        fclose(file);
        return true;
    }
    return false;
}

RC PagedFileManager::createFile(const string &fileName) {
    if (!fileName.empty() && !fileExist(fileName)) {
        FILE *file = fopen(fileName.c_str(), "wb+");
        fclose(file);
        return 0;       
    }
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName) {
    return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    if (!fileName.empty() && fileExist(fileName) && !fileHandle.pFile) {
        fileHandle.pFile = fopen(fileName.c_str(), "rb+");
        return 0;
    }
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (fileHandle.pFile) {
        fclose(fileHandle.pFile);
        fileHandle.pFile = NULL;	// avoid reopen error
        return 0;
    }
    return -1;
}


FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pFile = NULL;
}


FileHandle::~FileHandle() {
    pFile = NULL;
}


RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pFile) {
        fseek(pFile, pageNum * PAGE_SIZE, SEEK_SET);
        fread(data, 1, PAGE_SIZE, pFile);
        readPageCounter++;
        return 0;
    }
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pFile) {
        fseek(pFile, pageNum * PAGE_SIZE, SEEK_SET);
        fwrite(data, 1, PAGE_SIZE, pFile);
        writePageCounter++;
        return 0;
    }
    return -1;
}


RC FileHandle::appendPage(const void *data) {
    if (pFile) {
        fseek(pFile, 0, SEEK_END);
        fwrite(data, 1, PAGE_SIZE, pFile);
        appendPageCounter++;
        return 0;
    }
    return -1;
}


unsigned FileHandle::getNumberOfPages() {
    if (pFile) {
        fseek(pFile, 0, SEEK_END);
        return (unsigned int) (ftell(pFile) / PAGE_SIZE);
    }
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    if (pFile) {
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }
    return -1;
}
