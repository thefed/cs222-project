#include "rbfm.h"
#include <stdio.h>
#include <iostream>
#include "pfm.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

ofstream fcout; // Write debug info to file

short getRecordLength(const vector<Attribute> &recordDescriptor, const void *data, void *fieldPointer) {
    int fieldNum = recordDescriptor.size();			// the number of attributes
    bool nullBit = false;
    short offset = 0;
    short fieldOffset = 0;
    short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    int byte = 0;
    int bit = 0;

    offset += nullsIndicatorByteSize;

    for (int i = 0; i < fieldNum; i++) {
        nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        bit++;
        if (!nullBit) {
            fieldOffset = (fieldNum + 1) * sizeof(short) + offset;
            memcpy((char *) fieldPointer + i * sizeof(short), &fieldOffset, sizeof(short));
            if (recordDescriptor[i].type == 2) // TypeVarChar
            {
                short varCharLength;
                memcpy(&varCharLength, (char *) data + offset, sizeof(short));
                offset += varCharLength;
            }
            offset += 4; // TypeInt, TypeReal
        }
        if (bit % 8 == 0) {
            byte++;
            bit = 0;
        }
    }
    // offset for end of data
    fieldOffset = offset + (fieldNum + 1) * sizeof(short);
    memcpy((char *) fieldPointer + fieldNum * sizeof(short), &fieldOffset, sizeof(short));
    // Offset is the actual data length, not including the fieldsOffsetSize
    return offset;
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {

    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    int fieldNum = recordDescriptor.size();
    short fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    void *fieldsOffset = malloc(fieldsOffsetSize);
    void *curPageData = malloc(PAGE_SIZE);
    short ptrFreeSpace;
    short slotCount;
    short recordLength; // Actual record length [nullsIndicator, data]
    short recordOffset;
    recordLength = getRecordLength(recordDescriptor, data, fieldsOffset);

    bool appendPageFlag = true;
    if (fileHandle.getNumberOfPages() == 0) // No page exists, append a new one!
    {
        ptrFreeSpace = 0;
        slotCount = 0;
        // Insert fieldsOffset, record data
        memcpy((char *) curPageData + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
        memcpy((char *) curPageData + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
        // Update ptrFreeSpace, slotCount, slot(offset, length)
        recordOffset = ptrFreeSpace;
        recordLength += fieldsOffsetSize; // Record length inside the page [fields offset, nulls, data]
        ptrFreeSpace += recordLength;
        slotCount++;
        memcpy(((char *) curPageData + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
        memcpy(((char *) curPageData + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
        memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
        memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
        fileHandle.appendPage(curPageData);
        rid.pageNum = 0;
        rid.slotNum = slotCount - 1;
        fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **AP\n";
        appendPageFlag = false;
    }
    else // Find a page available
    {
        // Check current page
        fileHandle.readPage(fileHandle.getNumberOfPages() - 1, curPageData);
        memcpy(&ptrFreeSpace, (char *) curPageData + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
        short leftSpace = PAGE_SIZE - ptrFreeSpace - (2 * slotCount + 2) * sizeof(short);
        short insertedLength = fieldsOffsetSize + recordLength + 2 * sizeof(short);
        if (insertedLength <= leftSpace) 		// Free space is enough, insert it into current page
        {
            memcpy((char *) curPageData + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
            memcpy((char *) curPageData + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
            recordOffset = ptrFreeSpace;
            recordLength += fieldsOffsetSize; 			// Record length inside the page
            ptrFreeSpace += recordLength;
            slotCount++;
            memcpy((char *) curPageData + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));
            memcpy((char *) curPageData + PAGE_SIZE - 2 * sizeof(short), &slotCount, sizeof(short));
            memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
            memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
            fileHandle.writePage(fileHandle.getNumberOfPages() - 1, curPageData);
            rid.pageNum = fileHandle.getNumberOfPages() - 1;
            rid.slotNum = slotCount - 1;
            appendPageFlag = false;
            fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
            fcout << "offset: " << recordOffset << " length: " << recordLength << " **CP \n";
        }
        else // Free space is not enough, search for previous pages
        {
            for (unsigned i = 0; i < fileHandle.getNumberOfPages() - 1; i++) 
            {
                fileHandle.readPage(i, curPageData);
                memcpy(&ptrFreeSpace, (char *) curPageData + PAGE_SIZE - sizeof(short), sizeof(short));
                memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
                leftSpace = PAGE_SIZE - ptrFreeSpace - (2 * slotCount + 2) * sizeof(short);
                insertedLength = recordLength + fieldsOffsetSize + 2 * sizeof(short);
                if (insertedLength <= leftSpace) 
                {
                    memcpy((char *) curPageData + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
                    memcpy((char *) curPageData + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
                    recordOffset = ptrFreeSpace;
                    recordLength += fieldsOffsetSize; // Record length inside the page
                    ptrFreeSpace += recordLength;
                    slotCount++;
                    memcpy(((char *) curPageData + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
                    memcpy(((char *) curPageData + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
                    memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
                    memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
                    fileHandle.writePage(i, curPageData);
                    rid.pageNum = i;
                    rid.slotNum = slotCount - 1;

                    fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
                    fcout << "offset: " << recordOffset << " length: " << recordLength
                        << " **SP \n";
                    appendPageFlag = false;

                    break;
                }
            }
        }
    }
    if (appendPageFlag == true) // No space available for current record, append a new page
    {
        ptrFreeSpace = 0;
        slotCount = 0;
        short recordOffset = 0;
        // Insert fieldsOffset, record data
        memcpy((char *) curPageData + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
        memcpy((char *) curPageData + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
        // Update ptrFreeSpace, slotCount, slot(offset, length)
        recordLength += fieldsOffsetSize; // Record length inside the page
        ptrFreeSpace += recordLength;
        slotCount++;
        memcpy(((char *) curPageData + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
        memcpy(((char *) curPageData + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
        memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
        memcpy((char *) curPageData + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
        fileHandle.appendPage(curPageData);
        rid.pageNum = fileHandle.getNumberOfPages() - 1;
        rid.slotNum = slotCount - 1;

        fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **NAP \n";
    }
    free(fieldsOffset);
    free(curPageData);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    RC rc = -1;
    short fieldNum = recordDescriptor.size();
    short fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    short recordOffset;
    short recordLength;
    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, curPageData);

    memcpy(&recordOffset, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    memcpy(&recordLength, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));
    if (recordLength >= 0) 
    {
        memcpy(data, (char *) curPageData + recordOffset + fieldsOffsetSize, recordLength - fieldsOffsetSize);
        rc = 0;
        fcout << "readRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **curPageData\n";

    }
    else if (recordLength == -2) // Record updated and moved to somewhere else, get the new Rid and read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) curPageData + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) curPageData + recordOffset + sizeof(unsigned), sizeof(unsigned));
        fcout << "Record moved to page[" << newRid.pageNum << "][" << newRid.slotNum << "], ";
        rc = readRecord(fileHandle, recordDescriptor, newRid, data);

    }
    else // Record deleted, read failed!
    {
        fcout << "Record deleted! readRecord at page[" << rid.pageNum << "][" << rid.slotNum << "] failed!\n";
    }
    free(curPageData);
    return rc;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    if (!data) return -1;
    unsigned short offset = 0;
    bool nullBit;
    int byte = 0;
    int bit = 0;
    short fieldNum = recordDescriptor.size();
    short nullsIndicatorSize = ceil((double) fieldNum / CHAR_BIT);
    offset += nullsIndicatorSize;

    for (int i = 0; i < fieldNum; i++) 
    {
        nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        bit++;
        cout << recordDescriptor[i].name << " = ";
        if (!nullBit) 
        {
            if (recordDescriptor[i].type == 0) // TypeInt
            {
                cout << *(int *) (((char *) data) + offset) << "  ";
                offset += 4;
            }
            else if (recordDescriptor[i].type == 1) // TypeReal
            {
                cout << *(float *) (((char *) data) + offset) << "  ";
                offset += 4;
            }
            else // TypeVarChar
            {
                int varCharLength = *(int *) (((char *) data) + offset);
                offset += 4;
                for (int i = 0; i < varCharLength; i++) 
                {
                    cout << *((char *) data + offset);
                    offset ++;
                }
                cout << "  ";
            }
        }
        else
            cout << "NULL  ";
        if ((bit) % 8 == 0) 
        {
            byte++;
            bit = 0;
        }
    }
    cout << endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) 
{
    short ptrFreeSpace;
    short slotCount;
    short delRecordLength;
    short delRecordOffset;
    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, curPageData);

    memcpy(&delRecordLength, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));
    memcpy(&delRecordOffset, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));

    if (delRecordLength == -1) 
    {
        cout << "record in Page:" << rid.pageNum << " Slot:" << rid.slotNum << " has been deleted! Delete record failed!\n";
        free(curPageData);
        return -1;
    }
    else if (delRecordLength == -2) // Record has been moved, fetch new rid and delete it
    {
        RID newRid;		
        memcpy(&newRid.pageNum, (char *) curPageData + delRecordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) curPageData + delRecordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc;
        rc = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (rc == -1) // Moved record has been deleted as well, delete record failed!
        {
            free(curPageData);
            return -1;
        }
        delRecordLength = 2 * sizeof(unsigned);
    }

    memcpy(&ptrFreeSpace, (char *) curPageData + PAGE_SIZE - sizeof(short), sizeof(short));
    memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));

    if (rid.slotNum < (unsigned)slotCount - 1) // This is not the last record in page, to move its following records is needed!
    {
        // 1. move following records
        memmove((char *) curPageData + delRecordOffset, (char *) curPageData + delRecordOffset + delRecordLength, ptrFreeSpace - delRecordOffset - delRecordLength);
        // 2. Update slot directory for following records
        for (int i = rid.slotNum + 1; i < slotCount; i++) 
        {
            short curRecordOffset;
            memcpy(&curRecordOffset, (char *) curPageData + PAGE_SIZE - (2 * i + 4) * sizeof(short), sizeof(short));
            if (curRecordOffset != -1) // As long as record exists
            {		
                curRecordOffset -= delRecordLength;
                memcpy((char *) curPageData + PAGE_SIZE - (2 * i + 4) * sizeof(short), &curRecordOffset, sizeof(short));
            }
        }
    }
    // else: nothing to move

    // 3. Update free Space pointer
    ptrFreeSpace -= delRecordLength;
    memcpy((char *) curPageData + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));

    // 4. Set deleted record's offset and length to -1
    short mark = -1;
    memcpy((char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), &mark, sizeof(short));
    memcpy((char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &mark, sizeof(short));
    fileHandle.writePage(rid.pageNum, curPageData);
    free(curPageData);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    if (!data) return -1;
    RID curRid = rid;
    short fieldNum = recordDescriptor.size();
    short fieldsPointerSize = (fieldNum + 1) * sizeof(short);
    short oldRecordOffset;
    short oldRecordLength;
    short ptrFreeSpace;
    short slotCount;
    short updatedRecordLength;						// length of (updated data/tomb stone)

    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(curRid.pageNum, curPageData);

    memcpy(&oldRecordOffset, (char *) curPageData + PAGE_SIZE - (2 * curRid.slotNum + 4) * sizeof(short), sizeof(short));
    memcpy(&oldRecordLength, (char *) curPageData + PAGE_SIZE - (2 * curRid.slotNum + 3) * sizeof(short), sizeof(short));

    if (oldRecordLength == -1) // Record has been deleted, update failed!
    {
        free(curPageData);				
        return -1;
    }
    else if (oldRecordLength == -2) // Record has been moved, go to newRid to update!
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) curPageData + oldRecordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) curPageData + oldRecordOffset + sizeof(unsigned), sizeof(unsigned));
        cout << "Record moved, go to page[" << newRid.pageNum << "][" << newRid.slotNum << "] to update!\n";
        RC rc = updateRecord(fileHandle, recordDescriptor, data, newRid);
        free(curPageData);
        return rc;
    }

    memcpy(&ptrFreeSpace, (char *) curPageData + PAGE_SIZE - sizeof(short), sizeof(short));
    memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));

    void *tombStone = malloc(2 * sizeof(unsigned));
    void *fieldPointer = malloc(fieldsPointerSize);
    short lengthOfData = getRecordLength(recordDescriptor, data, fieldPointer);	// length of prepared data(replace old record)
    updatedRecordLength = lengthOfData + fieldsPointerSize;	// data.size needed to be written in, + fieldsPointer added to

    // Check if current page can hold updated record

    short increasedLength = updatedRecordLength - oldRecordLength;
    bool recordMovedFlag = false;
    if (ptrFreeSpace + increasedLength + (2 * slotCount + 2) * sizeof(short) > PAGE_SIZE) // Updated record is too long, insert it to another page
    {
        RID newRid;
        insertRecord(fileHandle, recordDescriptor, data, newRid);
        updatedRecordLength = 2 * sizeof(unsigned);
        increasedLength = updatedRecordLength - oldRecordLength; // increasedLength changed here!!!
        // Leave a tombstone
        memcpy(tombStone, &newRid.pageNum, sizeof(unsigned));
        memcpy((char *) tombStone + sizeof(unsigned), &newRid.slotNum, sizeof(unsigned));
        recordMovedFlag = true;
    }

    // move following records only when length changed and this is not the last record
    if (updatedRecordLength != oldRecordLength && rid.slotNum < (unsigned)slotCount - 1) 
    {
        // 1. move following records
        memmove((char *) curPageData + oldRecordOffset + updatedRecordLength, (char *)curPageData + oldRecordOffset + oldRecordLength, ptrFreeSpace - oldRecordOffset - oldRecordLength);
        // 2. Update record offset for following records
        for (int i = rid.slotNum + 1; i < slotCount; i++) 
        {
            short curRecordOffset;
            memcpy(&curRecordOffset, (char *) curPageData + PAGE_SIZE - (2 * i + 4) * sizeof(short), sizeof(short));
            if (curRecordOffset != -1) 
            {						// not have been deleted
                curRecordOffset += increasedLength;
                memcpy((char *) curPageData + PAGE_SIZE - (2 * i + 4) * sizeof(short), &curRecordOffset, sizeof(short));
            }
        }
    }
    // 3. Update ptrFreeSpace
    ptrFreeSpace += increasedLength;
    memcpy((char *) curPageData + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));

    // 4. Update slot directory of updated data
    if (recordMovedFlag == true) 
    {
        short mark = -2;
        memcpy((char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &mark, sizeof(short));
        memcpy((char *) curPageData + oldRecordOffset, tombStone, updatedRecordLength);
    }
    else 
    {
        memcpy((char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &updatedRecordLength, sizeof(short));
        memcpy((char *) curPageData + oldRecordOffset, fieldPointer, fieldsPointerSize);
        memcpy((char *) curPageData + oldRecordOffset + fieldsPointerSize, data, updatedRecordLength - fieldsPointerSize);
    }
    fileHandle.writePage(curRid.pageNum, curPageData);
    free(tombStone);
    free(fieldPointer);
    free(curPageData);
    return 0;
}

// Given attribute indexes, return corresponding attr data and nulls Indicator
RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const vector<int>returnAttrIndexes, void *data)
{
    // In case the sequence of indexes differ from those in the original record descriptor, readRecord() should not be called
    // if no attributes given, read failed!
    if (returnAttrIndexes.empty()) return -1;
    short recordOffset;
    short recordLength;
    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, curPageData);
    memcpy(&recordOffset, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    memcpy(&recordLength, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));

    if (recordLength == -1) // Record has been deleted, read attribute failed!
    {
        free(curPageData);
        return -1;
    }
    else if (recordLength == -2) // Record has been moved to a new place, fetch newRid and then read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) curPageData + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) curPageData + recordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc = readAttributes(fileHandle, recordDescriptor, newRid, returnAttrIndexes, data);
        free(curPageData);
        return rc;
    }

    int fieldNum = recordDescriptor.size();
    int returnAttrNum = returnAttrIndexes.size(); //projected Attribute numbers
    int fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    short returnedIndicatorSize = ceil((double) returnAttrNum / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) curPageData + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    unsigned char *rNullsIndicator = (unsigned char *) malloc(returnedIndicatorSize);
    memset(rNullsIndicator, 0, returnedIndicatorSize);

    unsigned short offset = returnedIndicatorSize;
    for (int i = 0; i < returnAttrNum; i++)
    {
        int byte, bit, rByte, rBit;
        byte = (int) floor((double) returnAttrIndexes[i] / CHAR_BIT);// returnindex[i]'s indicator located in byte of nullsIndicator
        bit = returnAttrIndexes[i] % CHAR_BIT;// located in bit'th bit of byte byte in nullsIndicator
        rByte = (int) floor((double) i / CHAR_BIT);				// current byte
        rBit = i % CHAR_BIT;									// current bit
        bool nullBit = nullsIndicator[byte] & (1 << (7 - bit));		// check return attribute indicator
        rNullsIndicator[rByte] |= (nullBit << (7 - rBit));	// assign current attribute's null bit
        if (!nullBit)
        {
            short attrOffset1, attrOffset2, fieldLength;
            memcpy(&attrOffset1, (char *) curPageData + recordOffset + returnAttrIndexes[i] * sizeof(short), sizeof(short));
            memcpy(&attrOffset2, (char *) curPageData + recordOffset + (returnAttrIndexes[i] + 1) * sizeof(short), sizeof(short));
            fieldLength = attrOffset2 - attrOffset1;
            memcpy((char *) data + offset, (char *) curPageData + recordOffset + attrOffset1, fieldLength);
            offset += fieldLength;
        }
    }
    // assert(fieldLength >= 0 && fieldLength <= PAGE_SIZE && "In RBFM::prepareReturnedData() fieldLength is invalid!\n");
    memcpy((char *) data, rNullsIndicator, returnedIndicatorSize); // Return nullsIndicator
    free(rNullsIndicator);
    free(nullsIndicator);
    free(curPageData);
    return 0;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) 
{
    if (attributeName.empty()) return -1; // attribute name is invalid, read attribute failed!
    int fieldNum = recordDescriptor.size();
    int fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    // Search attribute name in recrod descriptor
    int index = -1;
    for (int i = 0; i < fieldNum; i++) 
    {
        if (recordDescriptor[i].name == attributeName) 
        {
            index = i;
            break;
        }
    }
    if (index == -1 && attributeName !="") 
        return -1; // Attribute not found, read attribute failed!
    short recordOffset;
    short recordLength;
    void *curPageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, curPageData);
    memcpy(&recordOffset, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    memcpy(&recordLength, (char *) curPageData + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));

    if (recordLength == -1) // Record has been deleted, read attribute failed!
    {
        free(curPageData);
        return -1;
    }
    else if (recordLength == -2) // Record has been moved to a new place, fetch the new rid and then read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) curPageData + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) curPageData + recordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc = readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
        free(curPageData);
        return rc;
    }
    short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) curPageData + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    int byte = floor((double) index / CHAR_BIT); // decide which byte of null indicator
    int bit = index % 8;						// which bit of that byte
    bool nullBit = nullsIndicator[byte] & (1 << (7 - bit));

    unsigned char nullValue = nullBit << 7; // If nullBit = 1, return 1000 0000 (128)
    memcpy(data, &nullValue, 1);
    // Update at Nov. 5th, if the field is null, returned data is only nullIndicator, rc = 0
    unsigned short attrOffset1;
    unsigned short attrOffset2;
    memcpy(&attrOffset1, (char *) curPageData + recordOffset + index * sizeof(short), sizeof(short));
    memcpy(&attrOffset2, (char *) curPageData + recordOffset + (index + 1) * sizeof(short), sizeof(short));
    // Read attribute
    if (attrOffset2 > attrOffset1)
        memcpy((char *) data + 1, (char *) curPageData + recordOffset + attrOffset1, attrOffset2 - attrOffset1);
    free(nullsIndicator);
    free(curPageData);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.set(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator() 
{
    pageNum = 0;
    slotNum = 0;
    condAttrIndex = -1;
    compOp = NO_OP;
    value = NULL;
}

// Constructor with parameters
void RBFM_ScanIterator::set(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,
        const void *value,
        const vector<string> &attributeNames)
{
    pageNum = 0;
    slotNum = 0;
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttr = conditionAttribute;
    this->compOp = compOp;
    this->attributeNames = attributeNames;

    //Get index of the condition attribute
    if (conditionAttribute.empty())
    {
        for (int i = 0; i < (int)recordDescriptor.size(); i++)
        {
            if (recordDescriptor[i].name == conditionAttribute)
            {
                this->condAttrIndex = i;
                // Copy value pointer
                if (recordDescriptor[i].type == TypeInt)
                {
                    this->value = (int *)malloc(sizeof(int));
                    *((int *)this->value) =  *((int *)value);
                }
                else if (recordDescriptor[i].type == TypeReal)
                {
                    this->value = (float *)malloc(sizeof(float));
                    *((float *)this->value) =  *((float *)value);
                }
                else
                {
                    int valueLength;
                    memcpy(&valueLength, (char *)value, sizeof(int));
                    this->value = (char *)malloc(valueLength + sizeof(int));
                    memcpy((char *)this->value, (char *)value, valueLength + sizeof(int));
                }
                break;
            }
        }
    }
    else
        value = NULL;

    // Get indexes of projected attributes
    for (int i = 0; i < (int)attributeNames.size(); i++)
        for (int j = 0; j < (int)recordDescriptor.size(); j++)
            if (recordDescriptor[j].name == attributeNames[i])
            {
                this->returnAttrIndexes.push_back(j);
                break;
            }
}

RBFM_ScanIterator::~RBFM_ScanIterator() 
{
    if (condAttrIndex != -1 && !value)
        free(this->value);
    //	cout << "RBFM_ScanIterator destructed...\n";
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{ // Data is NULL, CompOp failed!
    if (compOp != NO_OP && !value) return -1;
    rid.pageNum = pageNum;
    rid.slotNum = slotNum;
    void *curPageData = malloc(PAGE_SIZE);
    short slotCount;
    fileHandle.readPage(rid.pageNum, curPageData);
    memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));

    RecordBasedFileManager *rbf = RecordBasedFileManager::instance();

    void *conditionValue = malloc(100);
    bool matchedFlag = false;
    int nameLength;
    string s;
    while (!matchedFlag) {
        rid.slotNum = slotNum;
        if (slotNum + 1 > slotCount) {
            pageNum ++;
            slotNum = 0;
            if (pageNum + 1 > (int)(fileHandle.getNumberOfPages())) {
                free(curPageData);
                free(conditionValue);
                return RBFM_EOF;
            }
            // Go to next page, update curPageData, slotCount
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;
            fileHandle.readPage(pageNum, curPageData);
            memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
        }
        short recordOffset;
        memcpy(&recordOffset, (char *) curPageData + PAGE_SIZE - (2*rid.slotNum + 4) * sizeof(short), sizeof(short));
        // Record exists
        if (recordOffset >= 0) {
            if (compOp == NO_OP)
                matchedFlag = true;
            else {
                RC rc = rbf->readAttribute(fileHandle, recordDescriptor, rid, conditionAttr, conditionValue);
                // conditionValue is NULL, compOp failed! Continue while loop
                if (rc == 0) {
                    AttrType curType = recordDescriptor[condAttrIndex].type;    // make name shorter
                    switch (compOp) {
                        case (EQ_OP):
                            if (curType == TypeInt) {
                               matchedFlag = *(int *) ((char *) conditionValue + 1) == *(int *) value;
                            }
                            else if (curType == TypeReal) {
                               matchedFlag = *(float *) ((char *) conditionValue + 1) == *(float *) value;
                            }
                            else {
                                 matchedFlag = *(int *) value == *(int *) ((char *) conditionValue + 1) && 
                                    !memcmp((char *)value + 4, (char *) conditionValue + 5, *(int *) value);
                            }
                            break;
                        case (LT_OP):
                            if (curType == TypeInt) {
                               matchedFlag = *(int *) ((char *) conditionValue + 1) < *(int *) value;
                            }
                            else if (curType == TypeReal) {
                               matchedFlag = *(float *) ((char *) conditionValue + 1) < *(float *) value;
                            }
                            else {
                                matchedFlag = (strcmp((char *) conditionValue + 5, (char *)value + 4) < 0);
                            }
                            break;
                        case (LE_OP):
                            if (curType == TypeInt) {
                               matchedFlag = *(int *) ((char *) conditionValue + 1) <= *(int *) value;
                            }
                            else if (curType == TypeReal) {
                               matchedFlag = *(float *) ((char *) conditionValue + 1) <= *(float *) value;
                            }
                            else {
                                matchedFlag = (strcmp((char *) conditionValue + 5, (char *)value + 4) <= 0);
                            }
                            break;
                        case (GT_OP):
                            if (recordDescriptor[condAttrIndex].type == TypeInt) 
                            {
                                if (*(int *) ((char *) conditionValue + 1) > *(int *) value) 
                                    matchedFlag = true;
                            }
                            else if (recordDescriptor[condAttrIndex].type == TypeReal)
                            {
                                if (*(float *) ((char *) conditionValue + 1) > *(float *) value) 
                                    matchedFlag = true;
                            }
                            else 
                            {
                                if (strcmp((char *) conditionValue + 5, (char *)value + 4) > 0)
                                    matchedFlag = true;
                            }
                            break;
                        case (GE_OP):
                            if (recordDescriptor[condAttrIndex].type == TypeInt) 
                            {
                                if (*(int *) ((char *) conditionValue + 1) >= *(int *) value) 
                                    matchedFlag = true;
                            }
                            else if (recordDescriptor[condAttrIndex].type == TypeReal)
                            {
                                if (*(float *) ((char *) conditionValue + 1) >= *(float *) value) 
                                    matchedFlag = true;
                            }
                            else 
                            {
                                if (strcmp((char *) conditionValue + 5, (char *)value + 4) >= 0)
                                    matchedFlag = true;
                            }
                            break;
                        case (NE_OP):
                            if (recordDescriptor[condAttrIndex].type == TypeInt) 
                            {
                                if (*(int *) ((char *) conditionValue + 1) != *(int *) value) 
                                    matchedFlag = true;
                            }
                            else if (recordDescriptor[condAttrIndex].type == TypeReal)
                            {
                                if (*(float *) ((char *) conditionValue + 1) != *(float *) value) 
                                    matchedFlag = true;
                            }
                            else 
                            {
                                if (strcmp((char *) conditionValue + 5, (char *)value + 4) != 0)
                                    matchedFlag = true;
                            }
                            break;
                    }
                }
            }
            if (matchedFlag) // Get data and exit while loop
            {
                RC rc = rbf->readAttributes(fileHandle, recordDescriptor, rid, returnAttrIndexes, data);
                assert(rc == 0 && "RBFM::readAttributes() should not fail!\n");
                // prepareReturnedData(fieldNum, returnAttrIndexes, curPageData, rid, data);
            }
        }
        slotNum ++;
    }
    free(conditionValue);
    free(curPageData);
    return 0;
}

RC RBFM_ScanIterator::close()
{
    RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
    rbf->closeFile(fileHandle);
    return 0;
}
