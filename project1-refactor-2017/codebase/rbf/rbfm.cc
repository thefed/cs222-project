#include "rbfm.h"
#include <stdio.h>
#include <string>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <cassert>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    // declare a class pointer (need to use its APIs)
    this->pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return this->pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return this->pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return this->pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return this->pfm->closeFile(fileHandle);
}

// inline functions to get/set slotCount and ptrFreeSpace, in case of change of design schema
inline int getSlotCount(const void *page) {
    return *(int *) ((char *) page + PAGE_SIZE - 2 * sizeof(int));
}

inline void setSlotCount(void *page, int slotCount) {
    *(int *) ((char *) page + PAGE_SIZE - 2 * sizeof(int)) = slotCount;
}

// ptr of free space getter and setter
inline int getPtrFreeSpace(const void *page) {
    return *(int *) ((char *) page + PAGE_SIZE - sizeof(int));   
}

inline void setPtrFreeSpace(void *page, int ptrFreeSpace) {
    *(int *) ((char *) page + PAGE_SIZE - sizeof(int)) = ptrFreeSpace;
}

// record length getter and setter
inline int getRecordLength(const void *page, int slotNum) {
    return *(int *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(int));
}

inline void setRecordLength(void *page, int slotNum, int recordLength) {
    *(int *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(int)) = recordLength;
}

// record offset getter and setter
inline int getRecordOffset(const void *page, int slotNum) {
    return *(int *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(int));
}

inline void setRecordOffset(void *page, int slotNum, int recordOffset) {
    *(int *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(int)) = recordOffset;   
}

/*
 * @param fieldOffsets: start offset pointer of each field, short type
 */
int getRecordLength(const vector<Attribute> &recordDescriptor, const void *data, void *fieldOffsets) {
    if (recordDescriptor.empty() || !data)        return 0;

    // record stored in page: offsets + header + data -> O(1) access of each field
    const int fieldCount = recordDescriptor.size();
    int offset = ceil(double(fieldCount) / CHAR_BIT);	// 1
    int byte = 0;
    int bit = 0;
    bool nullBit = false;
    for (unsigned i = 0; i < fieldCount; i++) {
        byte = i / 8;
        bit = i % 8;
        nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        if (!nullBit) {
            * ((short *) fieldOffsets + i) = short(offset + fieldCount * sizeof(short));
            if (recordDescriptor[i].type != TypeVarChar) {
                offset += 4;
            } else {
                int varLength =  *(int *)((char *)data + offset);
                offset += 4 + varLength;
            }
        }
    }
    return offset;
}


bool fillPage(void *page, const void *data, const void *fieldOffsets, const int &recordLength, const int &fieldOffsetsSize) {
    if (!page || !data || recordLength <= 0)        return false;
    int ptrFreeSpace = getPtrFreeSpace(page);
    int slotCount = getSlotCount(page);
    assert(ptrFreeSpace < PAGE_SIZE);
    assert(slotCount < PAGE_SIZE);

    // check if space is enough
    int leftSpace = PAGE_SIZE - ptrFreeSpace - (2 * slotCount + 2) * sizeof(int);	// slot dir + meta
    int insertedLength = recordLength + fieldOffsetsSize + 2 * sizeof(int);
    if (leftSpace > insertedLength) {
        slotCount++;
        assert(slotCount < PAGE_SIZE);
        setSlotCount(page, slotCount);

        setRecordOffset(page, slotCount - 1, ptrFreeSpace); // record offset: ptrFreeSpace
        setRecordLength(page, slotCount - 1, recordLength);

        memcpy((char *) page + ptrFreeSpace, fieldOffsets, fieldOffsetsSize);
        memcpy((char *) page + ptrFreeSpace + fieldOffsetsSize, data, recordLength);

        // update meta
        ptrFreeSpace += recordLength + fieldOffsetsSize;
        assert(ptrFreeSpace < PAGE_SIZE);
        setPtrFreeSpace(page, ptrFreeSpace);
        return true;
    }
    return false;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid){
    if (!fileHandle.pFile || recordDescriptor.empty() || !data)        return -1;

    const int fieldOffsetsSize = recordDescriptor.size() * sizeof(short);
    void *fieldOffsets = malloc(fieldOffsetsSize);
    assert(fieldOffsets);

    const int recordLength = getRecordLength(recordDescriptor, data, fieldOffsets);	// header + fields data
    bool needNewPage = true;
    void *page = malloc(PAGE_SIZE);
    assert(page);

    // append a new page
    if (fileHandle.getNumberOfPages() == 0) {
        if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
            fileHandle.appendPage(page);
            needNewPage = false;
        }
    }
    else {
        // check last page [n-1]
        const int numOfPages = fileHandle.getNumberOfPages();
        fileHandle.readPage(numOfPages - 1, page);
        if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
            fileHandle.writePage(numOfPages - 1, page);
            needNewPage = false;
        }
        // check previous pages [0..n-2]
        else {
            for (int i = 0; i < numOfPages - 1; i++) {
                memset(page, 0, PAGE_SIZE);
                fileHandle.readPage(i, page);
                if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
                    fileHandle.writePage(i, page);
                    int slotCount = getSlotCount(page);
                    rid.pageNum = i;
                    rid.slotNum = slotCount - 1;

                    assert(rid.slotNum < PAGE_SIZE);

                    free(fieldOffsets);
                    free(page);
                    return 0;
                }
            }
        }
    }
    if (needNewPage) {
        memset(page, 0, PAGE_SIZE);
        fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize);
        fileHandle.appendPage(page);
    }
    int slotCount = getSlotCount(page);
    rid.pageNum = fileHandle.getNumberOfPages() - 1;
    rid.slotNum = slotCount - 1;
    free(fieldOffsets);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    if (!fileHandle.pFile || recordDescriptor.empty()   // invalid file or descriptor or rid
        || rid.pageNum + 1 > fileHandle.getNumberOfPages()
        || rid.slotNum >= PAGE_SIZE) {
        return -1;
    }

    void *page = malloc(PAGE_SIZE);
    assert(page);
    
    if (fileHandle.readPage(rid.pageNum, page) == -1) return -1;
    
    const int recordOffset = getRecordOffset(page, rid.slotNum);
    const int recordLength = getRecordLength(page, rid.slotNum);
    const int fieldOffsetsSize = recordDescriptor.size() * sizeof(short);

    assert(recordOffset < PAGE_SIZE);
    assert(recordLength < PAGE_SIZE);

    if (recordOffset >= 0) {
        memcpy(data, (char *) page + recordOffset + fieldOffsetsSize, recordLength);
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    if (recordDescriptor.empty() || !data)  return -1;

    const int fieldSize = recordDescriptor.size();
    const int headerSize = ceil(double(fieldSize) / CHAR_BIT);
    bool nullBit = false;
    int offset = headerSize;
    for (unsigned i = 0; i < fieldSize; i++) {
        cout << recordDescriptor[i].name << ": ";
        int byte = i / CHAR_BIT;
        int bit = 7 - i % CHAR_BIT;
        nullBit = ((char *) data)[byte] & (1 << bit);
        if (!nullBit) {
            if (recordDescriptor[i].type == TypeInt) {
                cout << *(int *) ((char *) data + offset);
                offset += 4;
            }
            else if (recordDescriptor[i].type == TypeReal) {
                cout << *(float *) ((char *) data + offset);
                offset += 4;
            }
            else {
                int varCharlength = *(int *)((char *)data + offset);
                offset += 4;
                for (int j = 0; j < varCharlength; j++) {
                    cout << *((char *) data + offset + j);
                }
                offset += varCharlength;
            }
            cout << ", ";
        }
        else
            cout << "NULL, ";
    }
    cout << endl;
    return 0;
}
