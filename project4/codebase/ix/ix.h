#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <string.h>
#include <typeinfo>
#include "assert.h"
#include <stdlib.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// Utilities functions

template<class T>
int searchKeyInLeaf(void *data, int numKeys, T key);

int searchKeyInLeaf(void *data, int numKeys, char* key, const Attribute &attribute);

int searchKeyInIndex(void *data, int numIndex, char* key, const Attribute &attribute);

template<class T>
int searchKeyInIndex(void *data, int numIndex, T key);

void get_m1_InPage1(void *curPageData, int keyId, int insEntryLength, int &m1);

void removeEmptySlots(void *curPageData, int m, int slotCount);

void getOffsetIndex(void *curPageData, int m, int slotCount, int keyId, int &offsetIndex, int &entryOffset);

// Class definitions go here

class IXFileHandle;
class IX_ScanIterator;

// Get the root id of tree, always at the last 4 bytes at page 0
int getRootId(IXFileHandle &ixfileHandle);

// When root splits, set new root
void setRootId(IXFileHandle &ixfileHandle, const Attribute &attribute, int p0, const void *key, int p1);

template<class T>
void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, T &key, const RID &rid, T &newEntry_key, int &newEntry_pageId);

void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, char *key, const RID &rid, char *newEntry_key, int &newEntry_pageId);

class IndexManager {
    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);

        // Print a node given its id, used in print tree
        void printNode(IXFileHandle &ixfileHandle, const Attribute &attribute,const int nodeId, bool &isExistPrevKey);

//        void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, int &key, const RID &rid, IntIndex &newEntry);

//        void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, float &key, const RID &rid, FloatIndex &newEntry);
//
//        void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, char *key, const RID &rid, char *newKey, int &newPageId);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};

class IXFileHandle {
    public:
	FileHandle fileHandle;
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {
    public:
		IXFileHandle ixfileHandle; // Use all the functions of IXFileHandle class
		int leafPageStart; // Start leaf page number which contains the first matching key
		int leafPageEnd; // End leaf page number which contains the last matching key
		int keyStart; // Start key id of the first matching key
		int keyEnd; // End key id of the last matching key

		int leafPageNum; // Leaf page number counter
		// key id counter, is the k'th element, if accessing its offset, please go to find the k'th index of non-empty slot
		// eg: in slot dir{ 13, 25, -1, 37, -1, 49}, if validKeyId = 3, getNextEntry() should visit offset[5] = 49, not offset[2] = -1
		int validKeyId;

		Attribute attribute;
		void *lowKey;
		void *highKey;
		bool lowKeyInclusive;
		bool highKeyInclusive;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        // Set member values of ix_ScanIterator
        void set(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
        				const void *highKey, bool lowKeyInclusive, bool highKeyInclusive);

        RC getLowerBound(int pageId, const Attribute &attribute);

        RC getUpperBound(int pageId, const Attribute &attribute);

        // Int
        int searchKeyInLeaf_scan(void *data, int numKeys, int key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);

        // Float
        int searchKeyInLeaf_scan(void *data, int numKeys, float key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);

        // Varchar
        int searchKeyInLeaf_scan(void *data, int numKeys, char* key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);

        // Int
        int searchKeyInIndex_scan(void *data, int numIndex, int key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);

        // Float
        int searchKeyInIndex_scan(void *data, int numIndex, float key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);

        // Varchar
        int searchKeyInIndex_scan(void *data, int numIndex, char* key, bool isHighKey, bool Inclusive, bool &isKeyInIndex);
};

#endif
