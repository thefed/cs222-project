#include "rbfm.h"
#include <stdio.h>
#include <iostream>
#include "pfm.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

ofstream fcout; // Write debug info to file

// Print vector<string>
void print(const vector<string> &attrs, string msg)
{
    cout << msg << ": ";
    for (size_t i = 0; i < attrs.size(); i++)
    {
        if (i > 0) cout << ", ";
        cout << attrs[i];
    }
    cout << endl;
}

// Print vector<Attribute> Names
void print(const vector<Attribute> &attrs, string msg)
{
    cout << msg << ": ";
    for (size_t i = 0; i < attrs.size(); i++)
    {
        if (i > 0) cout << ", ";
        cout << attrs[i].name;
    }
    cout << endl;
}

void print(const vector<int>& v, string msg="") {
    cout << msg;
    for (int i : v) printf("%d ", i);
    printf("\n");
}

// Print void *data (without null header) given type
void printKey(const void *data, const AttrType type)
{
    if (type == TypeInt) {
        cout << *(int *) data << ", ";
    }
    else if (type == TypeReal) {
        cout << *(float *) data << ", ";
    }
    else
    {
        int len = *(int *) data;
        for (int i = 0; i < len; i++)
            cout << *((char *) data + 4 + i);
    }
    cout << endl;
}

// Compare string, where the first 4 bytes of 'key' pointer is the length chars followed
int strcmp_1(const char *key1, const char *key2)
{
    int l1 = *(int *)key1;
    int l2 = *(int *)key2;
    int l = min(l1, l2);
    int rc = memcmp(key1 + 4, key2 + 4, l);
    if (rc == 0)
    {
        if(l1 == l2)
            return rc;
        else if (l1 > l2)
            return 1;
        else
            return -1;
    }
    else
        return rc;
}

// Compare two attr.value (with null indicator)
bool isMatch(AttrType type, const void *value1, const void *value2, const CompOp &compOp)
{
    bool isMatch = false;
    if (compOp == NO_OP) return true;

    else if ((*(unsigned char *) value1) || (*(unsigned char *) value2)) // One val is NULL, compare failed
    {
        // cout << "Cannot compare with NULL\n";
        return false;
    }
    switch (compOp)
    {
        case (EQ_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) == *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) == *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) == 0)
                    isMatch = true;
            }
            break;
        case (LT_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) < *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) < *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) < 0)
                    isMatch = true;
            }
            break;
        case (LE_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) <= *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) <= *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) <= 0)
                    isMatch = true;
            }
            break;
        case (GT_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) > *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) > *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) > 0)
                    isMatch = true;
            }
            break;
        case (GE_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) >= *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) >= *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) >= 0)
                    isMatch = true;
            }
            break;
        case (NE_OP):
            if (type == TypeInt)
            {
                if (*(int *) ((char *) value1 + 1) != *(int *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else if (type == TypeReal)
            {
                if (*(float *) ((char *) value1 + 1) != *(float *) ((char *) value2 + 1))
                    isMatch = true;
            }
            else
            {
                if (strcmp_1((char *) value1 + 1, (char *) value2 + 1) != 0)
                    isMatch = true;
            }
            break;
        default:
            break;
    }
    return isMatch;
}

// Get the index of a given attrname in attrs
int getAttrIndex(const vector<Attribute> &attrs, const string &attrName) {
    for (size_t i = 0; i < attrs.size(); i++) {
        if (attrs[i].name == attrName) return i;
    }
    return -1;
}

// Get tuple length, containing nullsIndicator
int getTupleLength(const vector<Attribute> &attrs, const void *data)
{
    //int nullsIndicatorByteSize = ceil((double) attrs.size() / CHAR_BIT);
    int nullsIndicatorByteSize = getNullsSize(attrs.size());
    int offset = nullsIndicatorByteSize;
    for (size_t i = 0; i < attrs.size(); i++)
    {
        int byte = (int) floor((double) i / CHAR_BIT);
        int bit = i % CHAR_BIT;
        bool nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        if (!nullBit)
        {
            if (attrs[i].type == TypeVarChar)
            {
                int varlen = *(int *)((char *) data + offset);
                offset += (sizeof(int) + varlen);
            }
            else {
                offset += 4; // TypeInt, TypeReal
            }
        }
    }
    return offset;
}

// Get the length of tuple/record
int getTupleMaxSize(const vector<Attribute> &attrs)
{
    int size = 0;
    for (size_t i = 0; i < attrs.size(); i++)
        size += attrs[i].length;
    return size;
}

// Here void* data has no field offset, contains only nulls header and actual data
int readAttrFromTuple(const vector<Attribute> &attrs, const string &attrName, const void *data, void *value)
{
    int index = getAttrIndex(attrs, attrName);
    if (index == -1) return -1;     // target attr does not exist in descriptor

    //int nullsIndicatorByteSize = ceil((double) attrs.size() / CHAR_BIT);
    int nullsIndicatorByteSize = getNullsSize(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);

    memcpy(nullsIndicator, (char *) data, nullsIndicatorByteSize); // Get nullIndicator;
    int byte = floor((double) index / CHAR_BIT); // decide which byte of null indicator
    int bit = index % 8; // which bit of that byte
    bool nullBit = nullsIndicator[byte] & (1 << (7 - bit));

    *(unsigned char *)value = (nullBit << 7); // If nullBit = 1, return 1000 0000 (128)

    if (!nullBit) // Read attribute
    {
        int offset = nullsIndicatorByteSize;
        for (int i = 0; i < index; i++) // Get offset of target attr
        {
            if (attrs[i].type == TypeVarChar)
            {
                int len = *(int *)((char *)data + offset);
                offset += (sizeof(int) + len);
            }
            else
                offset += sizeof(int);
        }

        if (attrs[index].type == TypeVarChar)
        {
            int len = *(int *)((char *)data + offset);
            memcpy((char *) value + 1, (char *) data + offset, sizeof(int) + len);
        }
        else
            memcpy((char *) value + 1, (char *) data + offset, sizeof(int));
    }
    //	cout << "index = " << index << ", nullBit = " << nullBit << ", attrVal = " << *(int *)((char *)value + 1) << endl;

    free(nullsIndicator);
    return 0;
}

// Here void* data has no field offset, only nulls header and actual data
int readAttrsFromTuple(const vector<Attribute> &attrs, const vector<int> projAttrIndexes, const void *data, void *returnedData)
{
    int nullsIndicatorByteSize = ceil((double) attrs.size() / CHAR_BIT);
    int returnedIndicatorSize = ceil((double) projAttrIndexes.size() / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) data, nullsIndicatorByteSize); // Get nullIndicator;
    unsigned char *rNullsIndicator = (unsigned char *) malloc(returnedIndicatorSize);
    memset(rNullsIndicator, 0, returnedIndicatorSize);

    int rOffset = returnedIndicatorSize;
    for (unsigned i = 0; i < projAttrIndexes.size(); i++)
    {
        int byte, bit, rByte, rBit;
        byte = (int) floor((double) projAttrIndexes[i] / CHAR_BIT); // returnindex[i]'s indicator located in byte of nullsIndicator
        bit = projAttrIndexes[i] % CHAR_BIT; // located in bit'th bit of byte byte in nullsIndicator
        rByte = (int) floor((double) i / CHAR_BIT); // rent byte
        rBit = i % CHAR_BIT; // rent bit
        bool nullBit = nullsIndicator[byte] & (1 << (7 - bit)); // check return attribute indicator
        rNullsIndicator[rByte] |= (nullBit << (7 - rBit)); // assign rent attribute's null bit

        if (!nullBit) // Read attribute
        {
            int offset = nullsIndicatorByteSize;
            for (int j = 0; j < projAttrIndexes[i]; j++) // Get offset of target attr
            {
                if (attrs[j].type == TypeVarChar)
                {
                    int len = *(int *)((char *)data + offset);
                    offset += (sizeof(int) + len);
                }
                else
                    offset += sizeof(int);
            }

            if (attrs[projAttrIndexes[i]].type == TypeVarChar)
            {
                int len = *(int *)((char *)data + offset);
                memcpy((char *) returnedData + rOffset, (char *) data + offset, sizeof(int) + len);
                rOffset += (sizeof(int) + len);
            }
            else
            {
                memcpy((char *) returnedData + rOffset, (char *) data + offset, sizeof(int));
                rOffset += sizeof(int);
            }
        }
    }
    memcpy((char *) returnedData, rNullsIndicator, returnedIndicatorSize); // Return nullsIndicator

    free(rNullsIndicator);
    free(nullsIndicator);
    return 0;
}

short getRecordLength(const vector<Attribute> &recordDescriptor, const void *data, void *fieldPointer)
{
    int fieldNum = recordDescriptor.size(); // the number of attributes
    bool nullBit = false;
    short offset = 0;
    short fieldOffset = 0;
    //short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    short nullsIndicatorByteSize = (short) getNullsSize(fieldNum);
    int byte = 0;
    int bit = 0;

    offset += nullsIndicatorByteSize;

    for (int i = 0; i < fieldNum; i++)
    {
        nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        bit++;
        if (!nullBit)
        {
            fieldOffset = (fieldNum + 1) * sizeof(short) + offset;
            memcpy((char *) fieldPointer + i * sizeof(short), &fieldOffset, sizeof(short));
            if (recordDescriptor[i].type == 2) // TypeVarChar
            {
                short varCharLength = *(short *) ((char *) data + offset);
                offset += varCharLength;
            }
            offset += 4; // TypeInt, TypeReal
        }
        if (bit % 8 == 0)
        {
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

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    int fieldNum = recordDescriptor.size();
    short fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    void *fieldsOffset = malloc(fieldsOffsetSize);
    void *page = malloc(PAGE_SIZE);
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
        memcpy((char *) page + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
        memcpy((char *) page + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
        // Update ptrFreeSpace, slotCount, slot(offset, length)
        recordOffset = ptrFreeSpace;
        recordLength += fieldsOffsetSize; // Record length inside the page [fields offset, nulls, data]
        ptrFreeSpace += recordLength;
        slotCount++;

        setPtrFreeSpace(page, ptrFreeSpace);
        setSlotCount(page, slotCount);
        setRecordLength(page, slotCount - 1, recordLength);
        setRecordOffset(page, slotCount - 1, recordOffset);

        //memcpy(((char *) page + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
        //memcpy(((char *) page + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
        //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
        //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
        fileHandle.appendPage(page);
        rid.pageNum = 0;
        rid.slotNum = slotCount - 1;
        fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **AP\n";
        appendPageFlag = false;
    }
    else // Find a page available
    {
        // Check rent page
        fileHandle.readPage(fileHandle.getNumberOfPages() - 1, page);
        ptrFreeSpace = getPtrFreeSpace(page);
        slotCount = getSlotCount(page);
        //memcpy(&ptrFreeSpace, (char *) page + PAGE_SIZE - sizeof(short), sizeof(short));
        //memcpy(&slotCount, (char *) page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
        short leftSpace = PAGE_SIZE - ptrFreeSpace - (2 * slotCount + 2) * sizeof(short);
        short insertedLength = fieldsOffsetSize + recordLength + 2 * sizeof(short);
        if (insertedLength <= leftSpace) // Free space is enough, insert it into rent page
        {
            memcpy((char *) page + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
            memcpy((char *) page + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
            recordOffset = ptrFreeSpace;
            recordLength += fieldsOffsetSize; // Record length inside the page
            ptrFreeSpace += recordLength;
            slotCount++;

            setPtrFreeSpace(page, ptrFreeSpace);
            setSlotCount(page, slotCount);
            setRecordLength(page, slotCount - 1, recordLength);
            setRecordOffset(page, slotCount - 1, recordOffset);

            //memcpy((char *) page + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));
            //memcpy((char *) page + PAGE_SIZE - 2 * sizeof(short), &slotCount, sizeof(short));
            //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
            //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
            fileHandle.writePage(fileHandle.getNumberOfPages() - 1, page);
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
                fileHandle.readPage(i, page);
                memcpy(&ptrFreeSpace, (char *) page + PAGE_SIZE - sizeof(short), sizeof(short));
                memcpy(&slotCount, (char *) page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
                leftSpace = PAGE_SIZE - ptrFreeSpace - (2 * slotCount + 2) * sizeof(short);
                insertedLength = recordLength + fieldsOffsetSize + 2 * sizeof(short);
                if (insertedLength <= leftSpace)
                {
                    memcpy((char *) page + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
                    memcpy((char *) page + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
                    recordOffset = ptrFreeSpace;
                    recordLength += fieldsOffsetSize; // Record length inside the page
                    ptrFreeSpace += recordLength;
                    slotCount++;

                    setPtrFreeSpace(page, ptrFreeSpace);
                    setSlotCount(page, slotCount);
                    setRecordLength(page, slotCount - 1, recordLength);
                    setRecordOffset(page, slotCount - 1, recordOffset);

                    //memcpy(((char *) page + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
                    //memcpy(((char *) page + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
                    //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
                    //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
                    fileHandle.writePage(i, page);
                    rid.pageNum = i;
                    rid.slotNum = slotCount - 1;

                    fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
                    fcout << "offset: " << recordOffset << " length: " << recordLength << " **SP \n";
                    appendPageFlag = false;

                    break;
                }
            }
        }
    }
    if (appendPageFlag) // No space available for rent record, append a new page
    {
        ptrFreeSpace = 0;
        slotCount = 0;
        short recordOffset = 0;
        // Insert fieldsOffset, record data
        memcpy((char *) page + ptrFreeSpace, fieldsOffset, fieldsOffsetSize);
        memcpy((char *) page + ptrFreeSpace + fieldsOffsetSize, data, recordLength);
        recordLength += fieldsOffsetSize; // Record length inside the page
        ptrFreeSpace += recordLength;
        slotCount++;

        setPtrFreeSpace(page, ptrFreeSpace);
        setSlotCount(page, slotCount);
        setRecordLength(page, slotCount - 1, recordLength);
        setRecordOffset(page, slotCount - 1, recordOffset);

        //memcpy(((char *) page + PAGE_SIZE - sizeof(short)), &ptrFreeSpace, sizeof(short));
        //memcpy(((char *) page + PAGE_SIZE - 2 * sizeof(short)), &slotCount, sizeof(short));
        //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 1) * sizeof(short), &recordLength, sizeof(short));
        //memcpy((char *) page + PAGE_SIZE - (2 * slotCount + 2) * sizeof(short), &recordOffset, sizeof(short));
        fileHandle.appendPage(page);
        rid.pageNum = fileHandle.getNumberOfPages() - 1;
        rid.slotNum = slotCount - 1;

        fcout << "insertRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **NAP \n";
    }
    free(fieldsOffset);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    RC rc = -1;
    short fieldNum = recordDescriptor.size();
    short fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    short recordOffset;
    short recordLength;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    
    recordOffset = getRecordOffset(page, rid.slotNum);
    recordLength = getRecordLength(page, rid.slotNum);

    //memcpy(&recordOffset, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    //memcpy(&recordLength, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));
    if (recordLength >= 0)
    {
        memcpy(data, (char *) page + recordOffset + fieldsOffsetSize, recordLength - fieldsOffsetSize);
        rc = 0;
        fcout << "readRecord at page[" << rid.pageNum << "][" << rid.slotNum << "], ";
        fcout << "offset: " << recordOffset << " length: " << recordLength << " **page\n";

    }
    else if (recordLength == -2) // Record updated and moved to somewhere else, get the new Rid and read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) page + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) page + recordOffset + sizeof(unsigned), sizeof(unsigned));
        fcout << "Record moved to page[" << newRid.pageNum << "][" << newRid.slotNum << "], ";
        rc = readRecord(fileHandle, recordDescriptor, newRid, data);

    }
    else // Record deleted, read failed!
    {
        fcout << "Record deleted! readRecord at page[" << rid.pageNum << "][" << rid.slotNum << "] failed!\n";
    }
    free(page);
    return rc;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    if (!data) return -1;
    unsigned short offset = 0;
    int byte = 0;
    int bit = 0;
    //short nullsIndicatorSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    short nullsIndicatorSize = (short) getNullsSize(recordDescriptor.size());
    offset += nullsIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        bool nullBit = ((char *) data)[byte] & 1 << (7 - bit);
        bit++;
        cout << recordDescriptor[i].name << " = ";
        if (!nullBit)
        {
            if (recordDescriptor[i].type == TypeInt)
            {
                cout << *(int *) (((char *) data) + offset) << "  ";
                offset += 4;
            }
            else if (recordDescriptor[i].type == TypeReal)
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
                    offset++;
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
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    delRecordOffset = getRecordOffset(page, rid.slotNum);
    delRecordLength = getRecordLength(page, rid.slotNum);
    //memcpy(&delRecordLength, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));
    //memcpy(&delRecordOffset, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));

    if (delRecordLength == -1)
    {
        cout << "record in Page:" << rid.pageNum << " Slot:" << rid.slotNum << " has been deleted! Delete record failed!\n";
        free(page);
        return -1;
    }
    else if (delRecordLength == -2) // Record has been moved, fetch new rid and delete it
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) page + delRecordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) page + delRecordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc;
        rc = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (rc == -1) // Moved record has been deleted as well, delete record failed!
        {
            free(page);
            return -1;
        }
        delRecordLength = 2 * sizeof(unsigned);
    }

    ptrFreeSpace = getPtrFreeSpace(page);
    slotCount = getSlotCount(page);

    //memcpy(&ptrFreeSpace, (char *) page + PAGE_SIZE - sizeof(short), sizeof(short));
    //memcpy(&slotCount, (char *) page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));

    if (rid.slotNum < slotCount - 1) // This is not the last record in page, to move its following records is needed!
    {
        // 1. move following records
        memmove((char *) page + delRecordOffset, (char *) page + delRecordOffset + delRecordLength,
                ptrFreeSpace - delRecordOffset - delRecordLength);
        // 2. Update slot directory for following records
        for (int i = rid.slotNum + 1; i < slotCount; i++)
        {
            short RecordOffset = getRecordOffset(page, i);
            //memcpy(&RecordOffset, (char *) page + PAGE_SIZE - (2 * i + 4) * sizeof(short), sizeof(short));
            if (RecordOffset != -1) // As long as record exists
            {
                RecordOffset -= delRecordLength;
                setRecordOffset(page, i, RecordOffset);
                //memcpy((char *) page + PAGE_SIZE - (2 * i + 4) * sizeof(short), &curRecordOffset, sizeof(short));
            }
        }
    }
    // else: nothing to move

    // 3. Update free Space pointer
    ptrFreeSpace -= delRecordLength;
    setPtrFreeSpace(page, ptrFreeSpace);
    //memcpy((char *) page + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));

    // 4. Set deleted record's offset and length to -1
    short mark = -1;
    setRecordOffset(page, rid.slotNum, mark);
    setRecordLength(page, rid.slotNum, mark);
    //memcpy((char *) page + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), &mark, sizeof(short));
    //memcpy((char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &mark, sizeof(short));
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    if (data == NULL) // Check the validity of input data
        return -1;
    RID Rid = rid;
    short fieldNum = recordDescriptor.size();
    short fieldsPointerSize = (fieldNum + 1) * sizeof(short);
    short oldRecordOffset;
    short oldRecordLength;
    short ptrFreeSpace;
    short slotCount;
    short updatedRecordLength; // length of (updated data/tomb stone)

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(Rid.pageNum, page);

    oldRecordLength = getRecordLength(page, rid.slotNum);
    oldRecordOffset = getRecordOffset(page, rid.slotNum);
    //memcpy(&oldRecordOffset, (char *) page + PAGE_SIZE - (2 * curRid.slotNum + 4) * sizeof(short), sizeof(short));
    //memcpy(&oldRecordLength, (char *) page + PAGE_SIZE - (2 * curRid.slotNum + 3) * sizeof(short), sizeof(short));

    if (oldRecordLength == -1) // Record has been deleted, update failed!
    {
        free(page);
        return -1;
    }
    else if (oldRecordLength == -2) // Record has been moved, go to newRid to update!
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) page + oldRecordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) page + oldRecordOffset + sizeof(unsigned), sizeof(unsigned));
        cout << "Record moved, go to page[" << newRid.pageNum << "][" << newRid.slotNum << "] to update!\n";
        RC rc = updateRecord(fileHandle, recordDescriptor, data, newRid);
        free(page);
        return rc;
    }
    
    ptrFreeSpace = getPtrFreeSpace(page);
    slotCount = getSlotCount(page);
    //memcpy(&ptrFreeSpace, (char *) page + PAGE_SIZE - sizeof(short), sizeof(short));
    //memcpy(&slotCount, (char *) page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));

    void *tombStone = malloc(2 * sizeof(unsigned));
    void *fieldPointer = malloc(fieldsPointerSize);
    short lengthOfData = getRecordLength(recordDescriptor, data, fieldPointer); // length of prepared data(replace old record)
    updatedRecordLength = lengthOfData + fieldsPointerSize; // data.size needed to be written in, + fieldsPointer added to

    // Check if rent page can hold updated record

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
    if (updatedRecordLength != oldRecordLength && rid.slotNum < (unsigned) slotCount - 1)
    {
        // 1. move following records
        memmove((char *) page + oldRecordOffset + updatedRecordLength, (char *) page + oldRecordOffset + oldRecordLength,
                ptrFreeSpace - oldRecordOffset - oldRecordLength);
        // 2. Update record offset for following records
        for (int i = rid.slotNum + 1; i < slotCount; i++)
        {
            short RecordOffset = getRecordOffset(page, i);
            //memcpy(&RecordOffset, (char *) page + PAGE_SIZE - (2 * i + 4) * sizeof(short), sizeof(short));
            if (RecordOffset != -1)
            { // not have been deleted
                RecordOffset += increasedLength;
                setRecordOffset(page, i, RecordOffset);
                //memcpy((char *) page + PAGE_SIZE - (2 * i + 4) * sizeof(short), &curRecordOffset, sizeof(short));
            }
        }
    }
    // 3. Update ptrFreeSpace
    ptrFreeSpace += increasedLength;
    setPtrFreeSpace(page, ptrFreeSpace);
    //memcpy((char *) page + PAGE_SIZE - sizeof(short), &ptrFreeSpace, sizeof(short));

    // 4. Update slot directory of updated data
    if (recordMovedFlag == true)
    {
        short mark = -2;
        setRecordLength(page, rid.slotNum, mark);
        //memcpy((char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &mark, sizeof(short));
        memcpy((char *) page + oldRecordOffset, tombStone, updatedRecordLength);
    }
    else
    {
        setRecordLength(page, rid.slotNum, updatedRecordLength);
        //memcpy((char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), &updatedRecordLength, sizeof(short));
        memcpy((char *) page + oldRecordOffset, fieldPointer, fieldsPointerSize);
        memcpy((char *) page + oldRecordOffset + fieldsPointerSize, data, updatedRecordLength - fieldsPointerSize);
    }
    fileHandle.writePage(Rid.pageNum, page);
    free(tombStone);
    free(fieldPointer);
    free(page);
    return 0;
}

RC readAttrsFromPage(FileHandle &fileHandle, const void *page, const vector<Attribute> &recordDescriptor, const unsigned slotNum,
        const vector<int> returnAttrIndexes, void *data)
{
    // printf("readAttrsFromPage::");
    // print(recordDescriptor, "record descriptor_");
    // print(returnAttrIndexes, "returned indexes: ");

    // In case the sequence of indexes differ from those in the original record descriptor, readRecord() should not be called
    if (returnAttrIndexes.empty()) // No attributes given, read failed!
        return -1;

    short recordOffset = getRecordOffset(page, slotNum);
    short recordLength = getRecordLength(page, slotNum);
    //short recordOffset = *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short));
    //short recordLength = *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(short));
    if (recordLength == -1) // Record has been deleted, read attribute failed!
        return -1;
    else if (recordLength == -2) // Record has been moved to a new place, fetch newRid and then read again
    {
        unsigned pageNum = *(unsigned *)((char *) page + recordOffset);
        unsigned newSlotNum = *(unsigned *)((char *) page + recordOffset + sizeof(unsigned));
        void *newpage = malloc(PAGE_SIZE);
        fileHandle.readPage(pageNum, newpage);
        RC rc = readAttrsFromPage(fileHandle, newpage, recordDescriptor, newSlotNum, returnAttrIndexes, data);
        free(newpage);
        return rc;
    }

    int fieldNum = recordDescriptor.size();
    int returnAttrNum = returnAttrIndexes.size(); //projected Attribute numbers
    int fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    //short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    //short returnedIndicatorSize = ceil((double) returnAttrNum / CHAR_BIT);
    short nullsIndicatorByteSize = (short) getNullsSize(fieldNum);
    short returnedIndicatorSize = (short) getNullsSize(returnAttrNum);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) page + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    unsigned char *rNullsIndicator = (unsigned char *) malloc(returnedIndicatorSize);
    memset(rNullsIndicator, 0, returnedIndicatorSize);

    unsigned short offset = returnedIndicatorSize;
    for (int i = 0; i < returnAttrNum; i++)
    {
        int byte, bit, rByte, rBit;
        byte = (int) floor((double) returnAttrIndexes[i] / CHAR_BIT); // returnindex[i]'s indicator located in byte of nullsIndicator
        bit = returnAttrIndexes[i] % CHAR_BIT; // located in bit'th bit of byte byte in nullsIndicator
        rByte = (int) floor((double) i / CHAR_BIT); // rent byte
        rBit = i % CHAR_BIT; // rent bit
        bool nullBit = nullsIndicator[byte] & (1 << (7 - bit)); // check return attribute indicator
        rNullsIndicator[rByte] |= (nullBit << (7 - rBit)); // assign rent attribute's null bit
        if (!nullBit)
        {
            short attrOffset1, attrOffset2, fieldLength;
            memcpy(&attrOffset1, (char *) page + recordOffset + returnAttrIndexes[i] * sizeof(short), sizeof(short));
            memcpy(&attrOffset2, (char *) page + recordOffset + (returnAttrIndexes[i] + 1) * sizeof(short), sizeof(short));
            fieldLength = attrOffset2 - attrOffset1;
            assert(fieldLength >= 0);
            memcpy((char *) data + offset, (char *) page + recordOffset + attrOffset1, fieldLength);
            offset += fieldLength;
        }
    }
    // assert(fieldLength >= 0 && fieldLength <= PAGE_SIZE && "In RBFM::prepareReturnedData() fieldLength is invalid!\n");
    memcpy((char *) data, rNullsIndicator, returnedIndicatorSize); // Return nullsIndicator
    free(rNullsIndicator);
    free(nullsIndicator);
    return 0;
}

// Given attribute indexes, return corresponding attr data and nulls Indicator
RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
        const vector<int> returnAttrIndexes, void *data)
{
    // In case the sequence of indexes differ from those in the original record descriptor, readRecord() should not be called
    if (returnAttrIndexes.empty()) // No attributes given, read failed!
        return -1;

    short recordOffset;
    short recordLength;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    recordOffset = getRecordOffset(page, rid.slotNum);
    recordLength = getRecordLength(page, rid.slotNum);
    //memcpy(&recordOffset, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    //memcpy(&recordLength, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));

    if (recordLength == -1) // Record has been deleted, read attribute failed!
    {
        free(page);
        return -1;
    }
    else if (recordLength == -2) // Record has been moved to a new place, fetch newRid and then read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) page + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) page + recordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc = readAttributes(fileHandle, recordDescriptor, newRid, returnAttrIndexes, data);
        free(page);
        return rc;
    }

    int fieldNum = recordDescriptor.size();
    int returnAttrNum = returnAttrIndexes.size(); //projected Attribute numbers
    int fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    //short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    //short returnedIndicatorSize = ceil((double) returnAttrNum / CHAR_BIT);
    short nullsIndicatorByteSize = (short) getNullsSize(fieldNum);
    short returnedIndicatorSize = (short) getNullsSize(returnAttrNum);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) page + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    unsigned char *rNullsIndicator = (unsigned char *) malloc(returnedIndicatorSize);
    memset(rNullsIndicator, 0, returnedIndicatorSize);

    unsigned short offset = returnedIndicatorSize;
    for (int i = 0; i < returnAttrNum; i++)
    {
        int byte, bit, rByte, rBit;
        byte = (int) floor((double) returnAttrIndexes[i] / CHAR_BIT); // returnindex[i]'s indicator located in byte of nullsIndicator
        bit = returnAttrIndexes[i] % CHAR_BIT; // located in bit'th bit of byte byte in nullsIndicator
        rByte = (int) floor((double) i / CHAR_BIT); // rent byte
        rBit = i % CHAR_BIT; // rent bit
        bool nullBit = nullsIndicator[byte] & (1 << (7 - bit)); // check return attribute indicator
        rNullsIndicator[rByte] |= (nullBit << (7 - rBit)); // assign rent attribute's null bit
        if (!nullBit)
        {
            short attrOffset1, attrOffset2, fieldLength;
            memcpy(&attrOffset1, (char *) page + recordOffset + returnAttrIndexes[i] * sizeof(short), sizeof(short));
            memcpy(&attrOffset2, (char *) page + recordOffset + (returnAttrIndexes[i] + 1) * sizeof(short), sizeof(short));
            fieldLength = attrOffset2 - attrOffset1;
            memcpy((char *) data + offset, (char *) page + recordOffset + attrOffset1, fieldLength);
            offset += fieldLength;
        }
    }
    // assert(fieldLength >= 0 && fieldLength <= PAGE_SIZE && "In RBFM::prepareReturnedData() fieldLength is invalid!\n");
    memcpy((char *) data, rNullsIndicator, returnedIndicatorSize); // Return nullsIndicator
    free(rNullsIndicator);
    free(nullsIndicator);
    free(page);
    return 0;
}

RC readAttrFromPage(FileHandle &fileHandle, const void *page, const vector<Attribute> &recordDescriptor,
        const unsigned slotNum, const string &attributeName, void *data)
{
    int index = getAttrIndex(recordDescriptor, attributeName);
    if (index == -1 && attributeName.empty()) 
        return -1; // Attribute not found, read attribute failed!

    short recordOffset = getRecordOffset(page, slotNum);
    short recordLength = getRecordLength(page, slotNum);
    //short recordOffset = *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short));
    //short recordLength = *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(short));
    if (recordLength == -1) // Record has been deleted, read attribute failed!
        return -1;
    else if (recordLength == -2) // Record has been moved to a new place, fetch the new rid and then read again
    {
        int pageNum = *(unsigned *)((char *) page + recordOffset);
        int newSlotNum = *(unsigned *)((char *) page + recordOffset + sizeof(unsigned));
        void *newpage = malloc(PAGE_SIZE);
        fileHandle.readPage(pageNum, newpage);
        RC rc = readAttrFromPage(fileHandle, page, recordDescriptor, newSlotNum, attributeName, data);
        free(newpage);
        return rc;
    }
    //int nullsIndicatorByteSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    int nullsIndicatorByteSize = getNullsSize(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    int fieldsOffsetSize = (recordDescriptor.size() + 1) * sizeof(short);
    memcpy(nullsIndicator, (char *) page + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    int byte = floor((double) index / CHAR_BIT); // decide which byte of null indicator
    int bit = index % 8; // which bit of that byte
    bool nullBit = nullsIndicator[byte] & (1 << (7 - bit));

    unsigned char nullValue = nullBit << 7; // If nullBit = 1, return 1000 0000 (128)
    memcpy(data, &nullValue, 1);
    // Update at Nov. 5th, if the field is null, returned data is only nullIndicator, rc = 0
    unsigned short attrOffset1 = *(unsigned short *)((char *) page + recordOffset + index * sizeof(short));
    unsigned short attrOffset2 = *(unsigned short *)((char *) page + recordOffset + (index + 1) * sizeof(short));
    // Read attribute
    if (!nullValue) {
        // debug
        // printf("readAttrFromPage::read attr-name: %s, ", attributeName.c_str());
        // printf("offset1 = %d, offset2 = %d, key type = %d, value = .\n", attrOffset1, attrOffset2, recordDescriptor[index].type);
        // printf("offset1 = %d, offset2 = %d, key value = .\n", attrOffset1, attrOffset2);
        assert(attrOffset2 - attrOffset1 >= 0);
        // if (recordDescriptor[index].type == 0) {
        //     printf("readAttrFromPage:: recordDescriptor: \n");
        //     print(recordDescriptor);
        //     assert(attrOffset2 - attrOffset1 == 4);
        // }
        // if (attrOffset2 > attrOffset1)
        memcpy((char *) data + 1, (char *) page + recordOffset + attrOffset1, attrOffset2 - attrOffset1);
        // printKey(data, recordDescriptor[index].type);
        // printf("\n");
    }
    free(nullsIndicator);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid,
        const string &attributeName, void *data)
{
    if (attributeName.empty()) // Invalid attr name, readAttr failed.
        return -1;

    int fieldNum = recordDescriptor.size();
    int fieldsOffsetSize = (fieldNum + 1) * sizeof(short);
    // Search index of attribute name in record descriptor
    int index = getAttrIndex(recordDescriptor, attributeName);
    if (index == -1 && attributeName.empty()) // Given a valid attr name, but not found, read attribute failed!
        return -1;
    short recordOffset;
    short recordLength;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    recordOffset = getRecordOffset(page, rid.slotNum);
    recordLength = getRecordOffset(page, rid.slotNum);
    //memcpy(&recordOffset, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 4) * sizeof(short), sizeof(short));
    //memcpy(&recordLength, (char *) page + PAGE_SIZE - (2 * rid.slotNum + 3) * sizeof(short), sizeof(short));

    if (recordLength == -1) // Record has been deleted, read attribute failed!
    {
        free(page);
        return -1;
    }
    else if (recordLength == -2) // Record has been moved to a new place, fetch the new rid and then read again
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *) page + recordOffset, sizeof(unsigned));
        memcpy(&newRid.slotNum, (char *) page + recordOffset + sizeof(unsigned), sizeof(unsigned));
        RC rc = readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
        free(page);
        return rc;
    }
    //short nullsIndicatorByteSize = ceil((double) fieldNum / CHAR_BIT);
    short nullsIndicatorByteSize = (short) getNullsSize(fieldNum);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsIndicatorByteSize);
    memcpy(nullsIndicator, (char *) page + recordOffset + fieldsOffsetSize, nullsIndicatorByteSize); // Get nullIndicator;
    int byte = floor((double) index / CHAR_BIT); // decide which byte of null indicator
    int bit = index % 8; // which bit of that byte
    bool nullBit = nullsIndicator[byte] & (1 << (7 - bit));

    unsigned char nullValue = nullBit << 7; // If nullBit = 1, return 1000 0000 (128)
    memcpy(data, &nullValue, 1);
    // Update at Nov. 5th, if the field is null, returned data is only nullIndicator, rc = 0
    unsigned short attrOffset1;
    unsigned short attrOffset2;
    memcpy(&attrOffset1, (char *) page + recordOffset + index * sizeof(short), sizeof(short));
    memcpy(&attrOffset2, (char *) page + recordOffset + (index + 1) * sizeof(short), sizeof(short));
    // Read attribute
    if (attrOffset2 > attrOffset1)
        memcpy((char *) data + 1, (char *) page + recordOffset + attrOffset1, attrOffset2 - attrOffset1);
    free(nullsIndicator);
    free(page);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
        const CompOp compOp, // comparision type such as "<" and "="
        const void *value, // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator)
{
    if (fileHandle.pFile)
    {
        rbfm_ScanIterator.set(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        return 0;
    }
    else
    {
        cout << "In RBFM::scan(), NULL fileHandle!\n";
        return -1;
    }
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
    pageNum = 0;
    slotNum = 0;
    condAttrIndex = -1;
    compOp = NO_OP;
    compVal = malloc(PAGE_SIZE);
    page = malloc(PAGE_SIZE);
}

// Constructor with parameters
void RBFM_ScanIterator::set(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp,
        const void *value, const vector<string> &attributeNames)
{
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttr = conditionAttribute;
    this->compOp = compOp;
    this->attributeNames = attributeNames;

    if (fileHandle.getNumberOfPages() > 0)
        fileHandle.readPage(0, page);

    //Get index of the condition attribute
    condAttrIndex = getAttrIndex(recordDescriptor, conditionAttribute);
    if (condAttrIndex != -1) // Copy value pointer
    {
        *(unsigned char *) compVal = (0 << 7);
        if (recordDescriptor[condAttrIndex].type == TypeInt)
        {
            *(int *)((char *) compVal + 1) = *(int *) value;
        }
        else if (recordDescriptor[condAttrIndex].type == TypeReal)
        {
            *(float *)((char *) compVal + 1) = *(float *) value;
        }
        else
        {
            int valueLength = *(int *)value;
            memcpy((char *) compVal + 1, (char *) value, valueLength + sizeof(int));
            // debug
            // printf("RBFM_ScanIterator::set(): look for value: '%s' of length %d..., attr_name: %s\n", (char *) value + 4, valueLength, conditionAttribute.c_str());
        }
        //		cout << "RBFM_SI initialized, compVal = ";
        //		printKey((char *) compVal + 1, recordDescriptor[condAttrIndex].type);
    }
    else {
        printf("No condition attr index is found for attr: %s.\n", conditionAttribute.c_str());
    }

    // Get indexes of projected attributes
    for (unsigned i = 0; i < attributeNames.size(); i++)
    {
        int j = getAttrIndex(recordDescriptor, attributeNames[i]);
        this->returnAttrIndexes.push_back(j);
    }
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
    free(page);
    free(compVal);
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    bool noMatchingRecord = (fileHandle.getNumberOfPages() == 0) || (compOp != NO_OP && !compVal);
    if (noMatchingRecord) // Compare with NULL compVal should fail!
        return -1;
    //short slotCount = *(short *)((char *) page + PAGE_SIZE - 2 * sizeof(short));
    short slotCount = getSlotCount(page);

    void *conditionValue = malloc(PAGE_SIZE);
    bool isMatchFlag = false;
    while (!isMatchFlag)
    {
        if (slotNum > slotCount - 1) // Read next page, update page, slotCount
        {
            pageNum++;
            slotNum = 0;
            if (pageNum > (int) (fileHandle.getNumberOfPages()) - 1)
            {
                free(conditionValue);
                return -1;
            }
            fileHandle.readPage(pageNum, page);
            //slotCount = *(short *)((char *) page + PAGE_SIZE - 2 * sizeof(short));
            slotCount = getSlotCount(page);
        }
        //short recordOffset = *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short));
        short recordOffset = getRecordOffset(page, slotNum);
        if (recordOffset >= 0) // Record exists
        {
            if (compOp == NO_OP)
                isMatchFlag = true;
            else
            {
                int rc = readAttrFromPage(fileHandle, page, recordDescriptor, slotNum, conditionAttr, conditionValue);
                if (rc == 0) // Compare with NULL should fail
                {
                    // printf("getNextRecord:: condition key type = %d, value = ", recordDescriptor[condAttrIndex].type);
                    // printKey(conditionValue, recordDescriptor[condAttrIndex].type);
                    // printf("\n");
                    if (condAttrIndex == -1) isMatchFlag = true;
                    else 
                    isMatchFlag = isMatch(recordDescriptor[condAttrIndex].type, conditionValue, compVal, compOp);
                    //					cout << isMatchFlag << " getNextRecord() " << conditionAttr << ": ";// << recordDescriptor[1].name;
                    //					printKey((char *)conditionValue + 1, TypeVarChar);
                }
            }
            if (isMatchFlag) // Get data and exit while loop
            {
				// cout << "getNextRecord() matched!\n";// << recordDescriptor[1].name;
                //				printKey((char *)conditionValue + 1, recordDescriptor[1]);
                rid.pageNum = pageNum;
                rid.slotNum = slotNum;

                int rc = readAttrsFromPage(fileHandle, page, recordDescriptor, slotNum, returnAttrIndexes, data);

                // RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
                // printf("print record in getNextRecord: ");
                // vector<Attribute> attrs_;
                // for (auto idx : returnAttrIndexes) attrs_.push_back(recordDescriptor[idx]);
                // rbf->printRecord(attrs_, data);
                //				RC rc = rbf->readAttributes(fileHandle, recordDescriptor, rid, returnAttrIndexes, data);
                assert(rc == 0 && "readAttrsFromPage() should not fail!\n");
            }
        }
        slotNum++;
    }
    free(conditionValue);
    return 0;
}

RC RBFM_ScanIterator::close()
{
    RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
    rbf->closeFile(fileHandle);
    return 0;
}
