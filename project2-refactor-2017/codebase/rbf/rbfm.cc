#include "rbfm.h"
#include<stdio.h>
#include<string>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <cassert>

// rd: recordDescriptor
int getAttrIndex(const vector<Attribute> &rd, const string &attrName) {
    for (unsigned i = 0; i < rd.size(); i++) {
        if (rd[i].name == attrName) return i;
    }
    return -1;  // not found
}

RC readAttrsFromPage(FileHandle& fileHandle, const void *page, const vector<Attribute>& recordDescriptor, int slotNum, const vector<int> &projectedAttrIndexes, void *data) {
    return -1;
}

bool isMatch(AttrType type, const void *val1, const void *val2, const CompOp &compOp) {
    return false;
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
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
int calcRecordOffset(const vector<Attribute> &recordDescriptor, const void *data, void *fieldOffsets) {
    if (recordDescriptor.empty() || !data) return 0;

    // record stored in page: offsets + header + data -> O(1) access of each field
    const int fieldCount = recordDescriptor.size();
    int offset = ceil(double(fieldCount) / CHAR_BIT);	// 1
    int byte = 0;
    int bit = 0;
    bool nullBit = false;
    for (int i = 0; i < fieldCount; i++) {
        byte = i / 8;
        bit = i % 8;
        nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        if (!nullBit) {
            *((short *) fieldOffsets + i) = short(offset + fieldCount * sizeof(short));
            if (recordDescriptor[i].type != TypeVarChar) {
                offset += 4;    // sizeof(float), sizeof(int)
            } else {
                int varLength = *(int *)((char *)data + offset);
                offset += 4 + varLength;
            }
        }
    }
    return offset;
}


// try to fill 'page' with 'data', return false if failed
bool fillPage(void *page, const void *data, const void *fieldOffsets, int recordLength, int fieldOffsetsSize) {
    if (!page || !data || recordLength <= 0) return false;

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

        setRecordOffset(page, slotCount - 1, ptrFreeSpace);     // record offset: ptrFreeSpace
        setRecordLength(page, slotCount - 1, recordLength);

        memcpy((char *) page + ptrFreeSpace, fieldOffsets, fieldOffsetsSize);
        memcpy((char *) page + ptrFreeSpace + fieldOffsetsSize, data, recordLength);
        // update meta
        ptrFreeSpace += recordLength + fieldOffsetsSize;
        setPtrFreeSpace(page, ptrFreeSpace);
        return true;
    }
    return false;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid){
    if (!fileHandle.pFile || recordDescriptor.empty() || !data) return -1;

    const int fieldOffsetsSize = recordDescriptor.size() * sizeof(short);
    void *fieldOffsets = malloc(fieldOffsetsSize);
    const int recordLength = calcRecordOffset(recordDescriptor, data, fieldOffsets);	// header + fields data
    bool needNewPage = true;
    void *page = malloc(PAGE_SIZE);
    RC rc;
    // append a new page
    if (fileHandle.getNumberOfPages() == 0) {
        if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
            fileHandle.appendPage(page);
            needNewPage = false;
        }
    }
    else {
        // check last page
        const int numOfPages = fileHandle.getNumberOfPages();
        rc = fileHandle.readPage(numOfPages - 1, page);
        assert(rc == 0 && "readPage should not fail.");
        if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
            fileHandle.writePage(numOfPages - 1, page);
            needNewPage = false;
        }
        // check previous pages
        else {
            for (int i = 0; i < numOfPages - 1; i++) {
                memset(page, 0, PAGE_SIZE);
                rc = fileHandle.readPage(i, page);
                assert(rc == 0 && "readPage should not fail.");
                if (fillPage(page, data, fieldOffsets, recordLength, fieldOffsetsSize)) {
                    fileHandle.writePage(i, page);
                    // int slotCount = *(int *)((char *) page + PAGE_SIZE - 2 * 4);
                    int slotCount = getSlotCount(page);
                    rid.pageNum = i;
                    rid.slotNum = slotCount - 1;

                    assert(rid.slotNum < PAGE_SIZE && "invalid slotNum in checking previous page");

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
    if (!fileHandle.pFile || recordDescriptor.empty() || rid.pageNum + 1 > fileHandle.getNumberOfPages()
            || rid.slotNum >= PAGE_SIZE)
        return -1;
    void *page = malloc(PAGE_SIZE);
    assert(page);
    RC rc = fileHandle.readPage(rid.pageNum, page);
    assert(rc == 0 && "readPage should not fail.");

    const int fieldOffsetsSize = recordDescriptor.size() * sizeof(short);
    const int recordOffset = getRecordOffset(page, rid.slotNum);
    const int recordLength = getRecordLength(page, rid.slotNum);
    assert(recordOffset < PAGE_SIZE && "Read invalid record offset");
    assert(recordLength < PAGE_SIZE && "Read invalid record length");

    if (recordLength >= 0) {
        memcpy(data, (char *) page + recordOffset + fieldOffsetsSize, recordLength - fieldOffsetsSize);
        rc = 0;
    }
    else if (recordLength == -2) {  // record is updated and moved to a new place
        RID newRid;
        newRid.pageNum = *(int *)((char *)page + recordOffset);
        newRid.pageNum = *(int *)((char *)page + recordOffset + sizeof(int));
        rc = readRecord(fileHandle, recordDescriptor, newRid, data);
    }
    else {  // -1, record is deleted, failed
        printf("Read record at page [%d][%d] failed.\n", rid.pageNum, rid.slotNum);
    }
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    if (recordDescriptor.empty() || !data)        return -1;
    const int fieldSize = recordDescriptor.size();
    const int headerSize = ceil(double(fieldSize) / CHAR_BIT);
    bool nullBit = false;
    int offset = headerSize;
    for (int i = 0; i < fieldSize; i++) {
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

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    assert(rid.pageNum < fileHandle.getNumberOfPages());
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    int recordLength = getRecordLength(page, rid.slotNum);
    int recordOffset = getRecordOffset(page, rid.slotNum);

    if (recordLength == -1) { // target record is deleted already, repeated deletion should fail
        printf("repeated deletion of record at page[%d][%d]\n", rid.pageNum, rid.slotNum);
        free(page);
        return -1;
    }
    else if (recordLength == -2) { // record is moved, fetch new rid
        RID newRid;
        newRid.pageNum = *(int *)((char *)page + recordOffset);
        newRid.slotNum = *(int *)((char *)page + recordOffset + sizeof(int));
        RC rc = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (rc == -1) {
            free(page);
            return -1;
        }
        recordLength = 2 * sizeof(int);
        // after deleting the moved record at new location
        // 'recordLength' is used to clear up its old rid stub as well
    }

    int ptrFreeSpace = getPtrFreeSpace(page);
    int slotCount = getSlotCount(page);

    if (rid.slotNum < (unsigned) slotCount - 1) {
        // this is not the last record in this page
        // 1. move its following records forward/backward
        memmove((char *)page + recordOffset, (char *) page + recordOffset + recordLength, 
            ptrFreeSpace - recordOffset - recordLength);
        // 2. update slot directory for the following records
        for (int i = rid.slotNum + 1; i < slotCount; i++) {
            int curRecordOffset = getRecordOffset(page, i);
            if (curRecordOffset != -1) {
                curRecordOffset -= recordLength;
                setRecordOffset(page, i, curRecordOffset);
            }
        }

    }
    // else: nothing to move

    // 3. update ptr of free space
    ptrFreeSpace -= recordLength;
    setPtrFreeSpace(page, ptrFreeSpace);

    // 4. set deleted record's offset and length to -1
    int mark = -1;
    setRecordOffset(page, rid.slotNum, mark);
    setRecordLength(page, rid.slotNum, mark);

    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return -1;
}

  // Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    if (!data) return -1;

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    int recordOffset = getRecordOffset(page, rid.slotNum);
    int recordLength = getRecordLength(page, rid.slotNum);
    if (recordLength == -1) { // record is deleted, update failed
        printf("update record at page[%d][%d] failed (deleted already).\n", rid.pageNum, rid.slotNum);
        free(page);
        return -1;
    }
    else if (recordLength == -2) {
        // record is moved, fetch new rid
        RID newRid;
        newRid.pageNum = *(int *)((char *)page + recordOffset);
        newRid.slotNum = *(int *)((char *)page + recordOffset + sizeof(int));
        RC rc = updateRecord(fileHandle, recordDescriptor, data, newRid);
        free(page);
        return rc;
    }

    int ptrFreeSpace = getPtrFreeSpace(page);
    int slotCount = getSlotCount(page);
    // TODO
 
    free(page);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    return 0;
}

// Scan returns an iterator to allow the caller to go through the results one by one. 
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator) {
    // fileHandle need to be pre-binded with a file
    if (!fileHandle.pFile) return -1;
    rbfm_ScanIterator.set(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);  
    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
    page = malloc(PAGE_SIZE);
    compValue = malloc(PAGE_SIZE);
    compOp = NO_OP;
    pageNum = 0;
    slotNum = 0;
    condAttrIndex = -1;
}
RBFM_ScanIterator::~RBFM_ScanIterator() {
    free(page);
    free(compValue);
}

RC RBFM_ScanIterator::set(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames) {
    this->fileHandle = fileHandle;   // calls the assignment function
    this->recordDescriptor = recordDescriptor;
    this->conditionAttr = conditionAttribute;
    this->compOp = compOp;
    this->projectedAttrNames = attributeNames; 
    // initialize first page pointer
    if (fileHandle.getNumberOfPages() > 0) {
        fileHandle.readPage(0, page);
    }
    this->condAttrIndex = getAttrIndex(recordDescriptor, conditionAttribute);
    if (this->condAttrIndex != -1) {    // copy value to compValue
        AttrType type = recordDescriptor[condAttrIndex].type;
        if (type == TypeInt) {
            *(int *) compValue = *(int *) value; 
        }
        else if (type == TypeReal) {
            *(float *) compValue = *(float *) value; 
        } 
        else {
            memcpy(compValue, value, *(int *) value + sizeof(int));
        }
    }
    // calculate projected attrs indexes
    for (unsigned i = 0; i < attributeNames.size(); i++) {
        int idx = getAttrIndex(recordDescriptor, attributeNames[i]);
        // ignore invalid projected attributes
        if (idx != -1) this->projectedAttrIndexes.push_back(idx);   // in case wrong attrs are given
    }
    return 0;
}

RC readAttrFromPage(FileHandle &fileHandle, const void *page, const vector<Attribute> &recordDescriptor, int slotNum, const string &attributeName, void *data) {
    int index = getAttrIndex(recordDescriptor, attributeName);
    if (index == -1 || attributeName.empty()) {
        // attr not found in descriptor, failed
        return -1;
    }
    
    int recordOffset = getRecordOffset(page, slotNum);
    int recordLength = getRecordLength(page, slotNum);

    if (recordLength == -1) {   // record is deleted, fail
        return -1;
    }
    else if (recordLength == -2) {  // record is moved, fetch the new RID, read attr again
        int pageNum = *(unsigned *)((char *) page + recordOffset);
        int newSlotNum = *(unsigned *)((char *) page + recordOffset + sizeof(unsigned));
        void *newPage = malloc(PAGE_SIZE);
        fileHandle.readPage(pageNum, newPage);
        RC rc = readAttrFromPage(fileHandle, newPage, recordDescriptor, newSlotNum, attributeName, data);
        free(newPage);
        return rc;
    }
    // otherwise, record is placed here
    // copy the attribute value to 'data'

    return 0;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    const int maxPageNum = fileHandle.getNumberOfPages();
    bool noMatchedRecord = (maxPageNum == 0) || (compOp == NO_OP && !compValue);
    if (noMatchedRecord) {
        printf("No record: empty file or invalid input criteria\n");
        return -1;
    }

    // put the returned data in "data", update rid


    

    int slotCount = getSlotCount(page);
    void *conditionValue = malloc(PAGE_SIZE);
    bool isMatchFlag = false;   // true if a matching record is found

    while (!isMatchFlag) {
        // 1. check this->slotNum
        if (slotNum == slotCount) {
            pageNum++;
            slotNum = 0;
            // 2. check this->pageNum, e.g. maxPageNum=3, need pageNum <= 2
            if (pageNum == maxPageNum) {
                free(conditionValue);
                return -1;
            }
            // valid pageNum, fetch next page
            fileHandle.readPage(pageNum, page);
            slotCount = getSlotCount(page);
        }

        int recordOffset = *(int *) ((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(int));
        if (recordOffset >= 0) {    // record exists
            if (compOp == NO_OP) {
                isMatchFlag = true;
            }
            else {
                int rc = readAttrFromPage(fileHandle, page, recordDescriptor, slotNum, conditionAttr, conditionValue);
                if (rc == 0) {
                    isMatchFlag = isMatch(recordDescriptor[condAttrIndex].type, conditionValue, compValue, compOp);
                }
            }

            if (isMatchFlag) {  // get data and exit while
                rid.pageNum = pageNum;
                rid.slotNum = slotNum;
                int rc = readAttrsFromPage(fileHandle, page, recordDescriptor, slotNum, projectedAttrIndexes, data);
                assert(rc == 0 && "readAttrsFromPage() should not fail.");
            }
        }
        slotNum++;  // scan next record
    }
    free(conditionValue);
    return -1;
}

RC RBFM_ScanIterator::close() {
    assert(fileHandle.pFile);
    fclose(fileHandle.pFile);   // close the binded file
    fileHandle.pFile = NULL;    // unbind the fileHandle and the file
    return 0;
}
