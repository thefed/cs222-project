#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <fstream>
#include <math.h>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP	   // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator


// Utilities functions used by rbfm component's upper layers: rm, ix and qe

// Print vector<string>
void print(const vector<string>& attrs, string msg = "");

// Print vector<Attribute> Names
void print(const vector<Attribute>& attrs, string msg = "");

// Print void *data (without null header) of given type
void printKey(const void *data, const AttrType type);

// Compare string, where the first 4 bytes of 'key' pointer is the length chars followed
int strcmp_1(const char *key1, const char *key2);

// Compare two attr.value ('value' contains the null indicator)
bool isMatch(AttrType type, const void *value1, const void *value2, const CompOp &compOp);

// Get the index of a given attrname in attrs
int getAttrIndex(const vector<Attribute>& attrs, const string &attrName);

// Get tuple length, containing nullsIndicator
int getTupleMaxSize(const vector<Attribute>& attrs);

// Get the length of tuple/record
int getTupleLength(const vector<Attribute>& attrs, const void *data);

// Here void* data has no field offset, only nulls header and actual data
int readAttrFromTuple(const vector<Attribute> &attrs, const string &attrName, const void *data, void *value);

// Get projected attributes data
int readAttrsFromTuple(const vector<Attribute> &attrs, const vector<int> projAttrIndexes, const void *data, void *returnedData);

// inline function definitions

inline int getNullsSize(int fieldCount) { return ceil((double) fieldCount / CHAR_BIT); }

// inline functions to get/set slotCount and ptrFreeSpace, in case of change of design schema
inline short getSlotCount(const void *page) {
    return *(short *) ((char *) page + PAGE_SIZE - 2 * sizeof(short));
}

inline void setSlotCount(void *page, short slotCount) {
    *(short *) ((char *) page + PAGE_SIZE - 2 * sizeof(short)) = slotCount;
}

// ptr of free space getter and setter
inline short getPtrFreeSpace(const void *page) {
    return *(short *) ((char *) page + PAGE_SIZE - sizeof(short));   
}

inline void setPtrFreeSpace(void *page, short ptrFreeSpace) {
    *(short *) ((char *) page + PAGE_SIZE - sizeof(short)) = ptrFreeSpace;
}

// record length getter and setter
inline short getRecordLength(const void *page, int slotNum) {
    return *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(short));
}

inline void setRecordLength(void *page, int slotNum, short recordLength) {
    *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 3) * sizeof(short)) = recordLength;
}

// record offset getter and setter
inline short getRecordOffset(const void *page, int slotNum) {
    return *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short));
}

inline void setRecordOffset(void *page, int slotNum, short recordOffset) {
    *(short *)((char *) page + PAGE_SIZE - (2 * slotNum + 4) * sizeof(short)) = recordOffset;   
}



// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
	  vector<Attribute> recordDescriptor;
	  vector<string> attributeNames;
	  FileHandle fileHandle;
	  int pageNum;
	  int slotNum;
	  string conditionAttr;
	  int condAttrIndex;
	  CompOp compOp;
	  void *compVal; // compare value
	  vector<int> returnAttrIndexes;
	  void *page;
  RBFM_ScanIterator();
  void set(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const string &conditionAttribute,
		  const CompOp compOp,
		  const void *value,
		  const vector<string> &attributeNames);

  ~RBFM_ScanIterator();

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close();
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);
  RC readAttributes(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const vector<int>returnAttrIndexes, void *data);
  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

#endif
