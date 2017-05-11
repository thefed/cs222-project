#include "ix.h"

bool isShowScan = false;
bool isShowIndex = false;
bool isShowCreateNew = false;
bool isShowCreateNewIndex = false;
bool isShowLeaf = false;
bool isGetBound = false;
bool isPrintNode = false;
bool isScanLeaf = false;
bool isScanIndex = false;

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

PagedFileManager *pfm = PagedFileManager::instance();

RC IndexManager::createFile(const string &fileName) {
	// create a B+tree index file with given name
	RC rc = pfm->createFile(fileName);
	return rc;
}

RC IndexManager::destroyFile(const string &fileName) {
	//delete index file
	RC rc = pfm->destroyFile(fileName);
	return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	// successful, then return IXFileHandle object (bundle)
	RC rc = pfm->openFile(fileName, ixfileHandle.fileHandle);
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	//close file indicated
	RC rc = pfm->closeFile(ixfileHandle.fileHandle);
	return rc;
}

// Compare string, the first 4 bytes is length
int strcmp_1(char *key1, char *key2)
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

// Return the valid entry offsets
int getValidEntryOffsets(void *data, vector<int> &entryOffsets)
{
	int m = *(int *)((char *)data + 1);
	int slotCount = *(int *)((char *)data + PAGE_SIZE - 3*sizeof(int));
	for (int i = 0; i < slotCount; i++)
	{
		int entryOffset = *(int *)((char *)data + PAGE_SIZE - (i+1 + 3)*4);
		if (entryOffset > 0)
			entryOffsets.push_back(entryOffset);
	}
	if (entryOffsets.size() != m)
		return -1;
	return 0;
}

void printDataEntry(IXFileHandle &ixfileHandle, int pageId, int keyId, const Attribute &attribute)
{
	cout << "[" << pageId << "][" << keyId << "], ";
	void *data = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, data);
	RID rid;
	if (attribute.type == TypeInt)
	{
		int entryOffset = 1 + (3*keyId + 3)*sizeof(int);
		int key = *(int *)((char *)data + entryOffset);
		rid.pageNum = *(int *)((char *)data + entryOffset + 4);
		rid.slotNum = *(int *)((char *)data + entryOffset + 2*4);
		cout << "entry = [" << key << ": (" << rid.pageNum << ", " << rid.slotNum << ")]\n";
	}
	else if (attribute.type == TypeReal)
	{
		int entryOffset = 1 + (3*keyId + 3)*sizeof(int);
		int key = *(int *)((char *)data + entryOffset);
		rid.pageNum = *(int *)((char *)data + entryOffset + 4);
		rid.slotNum = *(int *)((char *)data + entryOffset + 2*4);
		cout << "entry = [" << key << ": (" << rid.pageNum << ", " << rid.slotNum << ")]\n";
	}
	else // (attribute.type == TypeVarChar)
	{
		vector<int> entryOffsets;
		getValidEntryOffsets(data, entryOffsets);
		int entryOffset = entryOffsets[keyId];
		int len = *(int *)((char *)data + entryOffset);
		char *key = (char *)malloc(len);
		memcpy(key, (char *)data + entryOffset + 4, len);
		entryOffset += (4 + len);
		rid.pageNum = *(int *)((char *)data + entryOffset);
		rid.slotNum = *(int *)((char *)data + entryOffset + 4);
		cout << "entry = [";
//		for ( int j = 0; j < len; j++)
//			cout << *(key + j);
		cout << *key << len;
		cout << ": (" << rid.pageNum << ", " << rid.slotNum << ")]\n";
		free(key);
	}
	free(data);
}

void insertRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	bool isLeafNode = true; // Flag of leaf page
	int m = 1; // Number of valid entries
	void *curPageData = malloc(PAGE_SIZE); // Data of a node
	int prevPageNum = -1;
	int nextPageNum = -1;
	int offset = 0;
	memcpy((char *)curPageData, &isLeafNode, 1);
	*(int *)((char *)curPageData + 1) = m; // Write flag, m, prev, next, key, rid
	offset = 1 + 4;
	*(int *)((char *)curPageData + offset) = prevPageNum;
	offset += 4;
	*(int *)((char *)curPageData + offset) = nextPageNum;
	offset += 4;
	int entryOffset = offset;
	if (attribute.type != TypeVarChar)
	{
		memcpy((char *)curPageData + offset, key, sizeof(int));
		offset += 4;
	}
	else
	{
		int varLength = *(int *)key;
		assert(varLength <= attribute.length && "insertRootId() invalid len");
		memcpy((char *)curPageData + offset, key, sizeof(int) + varLength);
		offset += (4 + varLength);
	}
	*(int *)((char *)curPageData + offset) = rid.pageNum;
	offset += 4;
	*(int *)((char *)curPageData + offset) = rid.slotNum;
	offset += 4;
	int ptrFreeSpace = offset;
	int slotCount = 1;
	*(int *)((char *)curPageData + PAGE_SIZE - 4*sizeof(int)) = entryOffset;
	*(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int)) = slotCount;
	*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
	int rootId = 0;
	*(int *)((char *)curPageData + PAGE_SIZE - sizeof(int)) = rootId;
	ixfileHandle.fileHandle.appendPage(curPageData); // Default page number = 0 (returned)
	free(curPageData);
}

//	int numKeys = *(int *)((char *)data + 1);
int searchKeyInLeaf(void *data, int numKeys, char* key, const Attribute &attribute)
{
//	if (*(key+4) == 't')
//		cout << "srch k = " << *(key+4) << endl;
	if (numKeys == 0) // No keys in leaf, search failed! Insert key at [0]
		return 0;
	vector<int> entryOffsets; // get the entry offsets
	getValidEntryOffsets(data, entryOffsets);

	int leftIndex = 0;
	int rightIndex = numKeys - 1;
	int entryOffset = *(int *)((char *)data + PAGE_SIZE - (leftIndex+1 + 3)*sizeof(int));
	int varLength = *(int *)((char *)data + entryOffset);
	assert(varLength <= attribute.length && "searchKeyInLeaf(char *) invalid varLength!");
	char *leftKey = (char *)malloc(varLength + 4);
	memcpy(leftKey, (char *)data + entryOffset, 4 + varLength);

	entryOffset = *(int *)((char *)data + PAGE_SIZE - (rightIndex+1 + 3)*sizeof(int));
	varLength = *(int *)((char *)data + entryOffset);
	char *rightKey = (char *)malloc(varLength + 4);
	memcpy(rightKey, (char *)data + entryOffset, 4 + varLength);

	int key_rightKey = strcmp_1(key, rightKey);
	int key_leftKey = strcmp_1(key, leftKey);
//	int key_rightKey = strcmp(key+4, rightKey+4);
//		int key_leftKey = strcmp(key+4, leftKey+4);

	if (key_rightKey >= 0)
	{
		free(leftKey);
		free(rightKey);
		return numKeys; // highKey, find the last key that is < or <= 285
	}
	else if (key_leftKey < 0) // eg: find the 1st key <= 285
	{
		free(leftKey);
		free(rightKey);
		return 0;
	}
	else // key[0] <= key <= key[m-1]
	{
		free(leftKey);
		free(rightKey);
		while (leftIndex + 1 < rightIndex)
		{
			int midIndex = (leftIndex + rightIndex) / 2;
			varLength = *(int *)((char *)data + entryOffsets[midIndex]);
			assert(varLength <= attribute.length && "seachKeyInLeaf(char *) invalid varLength");
			char *midKey = (char *)malloc(varLength + 4);
			memcpy(midKey, (char *)data + entryOffsets[midIndex], 4 + varLength);
			int key_midKey = strcmp_1(key, midKey);
			if (key_midKey < 0)
				rightIndex = midIndex;
			else if (key_midKey > 0)
				leftIndex = midIndex;
			else
			{
				free(midKey);
				return midIndex + 1;
			}
			free(midKey);
		}
		return rightIndex;
	}
}

// searchKeyInIndex returns the index of child pointer, ranging from [0, m]
int searchKeyInIndex(void *data, int numIndex, char* key, const Attribute &attribute)
{
	int leftIndex = 0;
	int rightIndex = numIndex - 1; // numIndex >= 1
	int entryOffset = *(int *)((char *)data + PAGE_SIZE - (leftIndex+1 + 3)*sizeof(int));
	int varLength = *(int *)((char *)data + entryOffset);
	assert(varLength <= attribute.length && "searchKeyInIndex() invalid varLength!");
	char *leftKey = (char *)malloc(varLength + 4);
	memcpy(leftKey, (char *)data + entryOffset, 4 + varLength);

	entryOffset = *(int *)((char *)data + PAGE_SIZE - (rightIndex+1 + 3)*sizeof(int));
	varLength = *(int *)((char *)data + entryOffset);
	char *rightKey = (char *)malloc(varLength + 4);
	memcpy(rightKey, (char *)data + entryOffset, 4 + varLength);

	if (strcmp_1(key, rightKey) >= 0)
	{
		free(leftKey);
		free(rightKey);
		return numIndex;
	}
	else if (strcmp_1(key, leftKey) < 0)
	{
		free(leftKey);
		free(rightKey);
		return 0;
	}
	else
	{
		free(leftKey);
		free(rightKey);
		while (leftIndex + 1 < rightIndex)
		{
			int midIndex = (leftIndex + rightIndex) / 2;
			entryOffset = *(int *)((char *)data + PAGE_SIZE - (midIndex+1 + 3)*sizeof(int));
			varLength = *(int *)((char *)data + entryOffset);
			assert(varLength <= attribute.length && "searchKeyInIndex() invalid varLength!");
			char *midKey = (char *)malloc(varLength + 4);
			memcpy(midKey, (char *)data + entryOffset, 4 + varLength);
			int key_midKey = strcmp_1(key, midKey);
			if (key_midKey < 0)
				rightIndex = midIndex;
			else if (key_midKey > 0)
				leftIndex = midIndex;
			else
			{
				free(midKey);
				return midIndex + 1;
			}
			free(midKey);
		}
		return rightIndex;
	}
}

RC createNewLeaf(void *curPageData, int m, int m1, int pageId, int keyId, int insEntryLength, char *key, const RID &rid, void *newPageData, const Attribute &attribute)
{
	int k = 3;
	bool isLeafNode = true;
	int m_new = m + 1 - m1; // the value of m, before insertion
	if (isShowCreateNew)
		cout << "Create new leaf... m = " << m << ", m1 = " << m1 << ", m2 = " << m_new << ", pageId = " << pageId << ", keyId = " << keyId << endl;

	int offset1; // the first entry offset in leaf_2
	int offset2 = 1 + k*sizeof(int);
	memcpy((char *)newPageData, &isLeafNode, 1); // flag
	*(int *)((char *)newPageData + 1) = m_new; // m
	*(int *)((char *)newPageData + 1 + sizeof(int)) = pageId; // prev
	int next = *(int *)((char *)curPageData + 1 + 2*sizeof(int)); // next
	*(int *)((char *)newPageData + 1 + 2*sizeof(int)) = next;

	// Copy m_new data entries one by one
	int validCp = 0;
	if (keyId < m1) // new key will be in leaf_1, then copy from [m1-1]
	{
		offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 3)*sizeof(int)); // m = 140, m1 = 85, m2 = 56, kI = 65
		for (int i = m1 - 1; i < m; i++) // m = 140, the last offset is oF[139], copy from [84], keep [0]-[83]
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length )
			{
				cout << "CreateNewLeaf(): Invalid varLength in leaf[" << pageId << "], i = " << i << "\n";
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
//			cout << "NewLeaf: copy and create key[" << i - (m1 - 1) << "], o1 = " << offset1 << ", o2 = " << offset2
//					<< ", key = " << *((char *)curPageData + offset1 + 4) << varLength
//					<< ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			memcpy((char *)newPageData + PAGE_SIZE - (i+1 - m1 + 1+3)*sizeof(int), &offset2, sizeof(int));
			offset2 += entryLength;
			offset1 += entryLength;
		}
	}
	else
	{
		offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 1+ 3)*sizeof(int)); // m = 140, m1 = 85, m2 = 56, kI > m1, copy from [85]
		// m = 111, m1 = 60, m2 = 52, keyId = 75, keep [0]-[59], copy key[60]-[74], to [0]-[14], copy [75]-[110] to [16] to [51]
		// [15] is for inserted keyId
		for (int i = m1; i < keyId; i++)
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length)
			{
				cout << "CreateNewLeaf(): Invalid varLength in leaf.prev=[" << pageId << "]\n";
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
//			cout << "NewLeaf: copy and create key[" << i - m1 << "], o1 = " << offset1 << ", o2 = " << offset2
//					<< ", key = " << ((char *)curPageData + offset1 + 4)[0] << varLength
//																		 << ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			*(int *)((char *)newPageData + PAGE_SIZE - (i - m1 + 1+3)*sizeof(int)) = offset2; // when i = m1, write offset[0]
			offset2 += entryLength;
			offset1 += entryLength;
		}
		// get entry offset of inserted key
		*(int *)((char *)newPageData + PAGE_SIZE - (keyId - m1 + 1+3)*sizeof(int)) = offset2;
		// write inserted key to new leaf
		int varLength = *(int *)key;
		if (isShowCreateNew)
			cout << "NewLeaf: insert key[" << keyId - m1 << "], o2 = " << offset2 << ", varlen = " << varLength << *(key + 4)
				<< ", rid.p = " << rid.pageNum << endl;
		memcpy((char *)newPageData + offset2, key, 4 + varLength);
		memcpy((char *)newPageData + offset2 + 4 + varLength, &rid.pageNum, sizeof(int));
		memcpy((char *)newPageData + offset2 + 2*4 + varLength, &rid.slotNum, sizeof(int));
		offset2 += insEntryLength;
		for (int i = keyId; i < m; i++)
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length)
			{
				cout << "CreateNewLeaf(): Invalid varLength in leaf.prev=["
						<< pageId << "], i = " << i << ", offset = " << offset1 << endl;
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
			if (isShowCreateNew)
				cout << "NewLeaf: copy and create key[" << i - m1 + 1 << "], o1 = " << offset1 << ", o2 = " << offset2 << " varlen = " << varLength
					<< ", key = " << ((char *)curPageData + offset1 + 4)[0]
					                                                     << ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			*(int *)((char *)newPageData + PAGE_SIZE - (i - m1 +1 + 1+3)*sizeof(int)) = offset2; // when i = 75, write offset[75-60+1], skip [15]
			offset2 += entryLength;
			offset1 += entryLength;
		}
	}
	// Update slotCount, ptrFreeSpace
	*(int *)((char *)newPageData + PAGE_SIZE - 3*sizeof(int)) = m_new;
	*(int *)((char *)newPageData + PAGE_SIZE - 2*sizeof(int)) = offset2;
	if (isShowCreateNew)
		cout << validCp << " entries copied to new leaf\n";
	return 0;
}

RC createNewIndex(void *curPageData, int m, int m1, int pageId, int keyId, int insEntryLength, char *key, int &newPageId, void *newPageData, const Attribute &attribute)
{
	int k = 2;
	bool isLeafNode = false;
	int m_new = m - m1; // the value of m, before insertion
	if (isShowCreateNewIndex)
		cout << "Create new leaf... m = " << m << ", m1 = " << m1 << ", m2 = " << m_new << ", pageId = " << pageId << ", keyId = " << keyId << endl;
	int offset1; // the first entry offset in leaf_2
	int offset2 = 1 + k*sizeof(int);
	memcpy((char *)newPageData, &isLeafNode, 1); // flag
	*(int *)((char *)newPageData + 1) = m_new; // m
	// flag, m, p0, k1, p1, k2, p2, k3, p3, [k4], p4, k5, p5, k6, p6. suppose m1 = 3, m2 = 2, m = 5
	offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1+1 + 3)*sizeof(int));
	// Read from p4!!! not k4, move offset1 += (4+varLength)
	int varLength = *(int *)((char *)curPageData + offset1);
	offset1 += (4 + varLength);
	cout << "p4 = " << *(int *)((char *)curPageData + offset1) << endl;
	*(int *)((char *)newPageData + 5) = *(int *)((char *)curPageData + offset1); // copy p4
	offset1 += 4;
	offset2 += 4;
	int validCp = 0;
	if (keyId < m1) // new key will be in index_1, then copy from [m1-1]
	{
		offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 3)*sizeof(int)); // m = 140, m1 = 85, m2 = 56, kI = 65
		for (int i = m1; i < m; i++) // m = 140, the last offset is oF[139], copy from [84], keep [0]-[83]
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length)
			{
				cout << "CreateNewLeaf(): Invalid varLength in leaf[" << pageId << "]\n";
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
			if (isShowCreateNewIndex)
				cout << "NewIndex: copy and create key[" << i - (m1 - 1) << "], o1 = " << offset1 << ", o2 = " << offset2 << " varlen = " << varLength
					<< ", key = " << *((char *)curPageData + offset1 + 4)
					<< ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			memcpy((char *)newPageData + PAGE_SIZE - (i+1 - m1 + 1+3)*sizeof(int), &offset2, sizeof(int));
			offset2 += entryLength;
			offset1 += entryLength;
		}
	}
	else
	{
		offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 1 + 1+ 3)*sizeof(int)); // m = 140, m1 = 85, m2 = 56, kI > m1, copy from [85]
		// m = 111, m1 = 60, m2 = 52, keyId = 75, keep [0]-[59], copy key[60]-[74], to [0]-[14], copy [75]-[110] to [16] to [51]
		// [15] is for inserted keyId
		for (int i = m1 + 1; i < keyId; i++)
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length)
			{
				cout << "CreateNewIndex(): Invalid varLength in leaf.prev=[" << pageId << "]\n";
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
			if (isShowCreateNewIndex)
				cout << "NewIndex: copy and create key[" << i - m1 << "], o1 = " << offset1 << ", o2 = " << offset2 << " varlen = " << varLength
					<< ", key = " << ((char *)curPageData + offset1 + 4)[0]
					                                                     << ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			*(int *)((char *)newPageData + PAGE_SIZE - (i - m1 + 1+3)*sizeof(int)) = offset2; // when i = m1, write offset[0]
			offset2 += entryLength;
			offset1 += entryLength;
		}
		// get entry offset of inserted key
		*(int *)((char *)newPageData + PAGE_SIZE - (keyId - m1-1 + 1+3)*sizeof(int)) = offset2;
		// write inserted key to new leaf
		int varLength = *(int *)key;
		if (isShowCreateNewIndex)
			cout << "NewIndex: insert key[" << keyId - m1-1 << "], o2 = " << offset2 << ", varlen = " << varLength << *(key + 4) << endl;
		memcpy((char *)newPageData + offset2, key, 4 + varLength);
		memcpy((char *)newPageData + offset2 + 4 + varLength, &newPageId, sizeof(int));
		offset2 += insEntryLength;
		for (int i = keyId; i < m; i++)
		{
			int varLength = *(int *)((char *)curPageData + offset1);
			if (varLength > attribute.length)
			{
				cout << "CreateNewIndex(): Invalid varLength in leaf[" << pageId << "]\n";
				return -1;
			}
			else
				validCp++;
			int entryLength = k*4 + varLength; // For leaf, k = 3;
			if (isShowCreateNewIndex)
				cout << "NewLeaf: copy and create key[" << i - m1 + 1 << "], o1 = " << offset1 << ", o2 = " << offset2 << " varlen = " << varLength
					<< ", key = " << ((char *)curPageData + offset1 + 4)[0]
					                                                     << ", rid.p = " << *(int *)((char *)curPageData + offset1 + 4 + varLength) << endl;
			memcpy((char *)newPageData + offset2, (char *)curPageData + offset1, entryLength);
			*(int *)((char *)newPageData + PAGE_SIZE - (i - m1 +1 + 1+3)*sizeof(int)) = offset2; // when i = 75, write offset[75-60+1], skip [15]
			offset2 += entryLength;
			offset1 += entryLength;
		}
	}
	// Update slotCount, ptrFreeSpace
	memcpy((char *)newPageData + PAGE_SIZE - 3*sizeof(int), &m_new, sizeof(int));
	memcpy((char *)newPageData + PAGE_SIZE - 2*sizeof(int), &offset2, sizeof(int));
	return 0;
}

// Return the number of entries that page 1 should keep
void get_m1_InPage1(void *curPageData, int keyId, int insEntryLength, int &m1)
{
	int slotCount = *(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int));
	bool is_m1Found = false;
	for (int i = 0; i < keyId; i++)
	{
		int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int));
		if (entryOffset + (i+1)*4 >= PAGE_SIZE / 2 - 500) // In this case, inserted key will be loaded to new leaf
		{
			is_m1Found = true;
			m1 = i; // eg: i = 50, then entry_0 t0 entry_49 have taken up over 50%, m1 = 50
			break;
		}
	}
	if (!is_m1Found)
	{
		for (int i = keyId; i < slotCount; i++)
		{
			int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int));
			entryOffset += insEntryLength;
			if (entryOffset + (i+1)*4 >= PAGE_SIZE / 2 - 500) // In this case, inserted key will be in old leaf
			{
				m1 = i; // eg: i = 50, then entry_0 t0 entry_49 have taken up over 50%, m1 = 50
				break;
			}
		}
	}
}


void removeEmptySlots(void *curPageData, int m, int slotCount)
{
	if (m < slotCount)
	{
		for (int i = 0; i < slotCount; i++)
		{
			int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int));
			if (entryOffset < 0)
			{
				if (i < slotCount - 1)
				{
					int slotDirOffset = PAGE_SIZE - (slotCount + 3)*sizeof(int);
					memmove((char *)curPageData + slotDirOffset + 4, (char *)curPageData + slotDirOffset, (slotCount - i-1)*sizeof(int));
				}
				slotCount --;
				if (m == slotCount)
					break;
			}
		}
		memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int)); // Compress slotCount
	}
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid) {
	// attribute for key
	//int real use 4byte, char int-length+ char
	//not contain null flags
	// when overflow, restruct
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0)
	{
		insertRoot(ixfileHandle, attribute, key, rid);
		return 0;
	}
	int newEntry_pageId = -1;
	int rootId = getRootId(ixfileHandle);
	if (attribute.type == TypeInt)
	{
		int newEntry_key;
		insertKey(ixfileHandle, attribute, rootId, *(int *)key, rid, newEntry_key, newEntry_pageId);
	}
	else if (attribute.type == TypeReal)
	{
		float newEntry_key;
		insertKey(ixfileHandle, attribute, rootId, *(float *)key, rid, newEntry_key, newEntry_pageId);
	}
	else
	{
		char *newEntry_key = (char *)malloc(attribute.length + 4);
		insertKey(ixfileHandle, attribute, rootId, (char *)key, rid, newEntry_key, newEntry_pageId);
		free(newEntry_key);
	}
	return 0;
}

//void IndexManager::setRootId(IXFileHandle &ixfileHandle, const Attribute &attribute, int p0, const void *key, int p1)
void setRootId(IXFileHandle &ixfileHandle, const Attribute &attribute, int p0, const void *key, int p1)
{
	bool isLeafNode = false;
	int m = 1;
	int rootId;
	int offset;
	void *curPageData = malloc(PAGE_SIZE);

	memcpy((char *)curPageData, &isLeafNode, 1);
	memcpy((char *)curPageData + 1, &m, sizeof(int));
	offset = 1 + sizeof(int);
	memcpy((char *)curPageData + offset, &p0, sizeof(int)); // Write p0
	offset += 4;
	if (attribute.type != TypeVarChar) // TypeInt, TypeReal
	{
		memcpy((char *)curPageData + offset, key, sizeof(int)); // Write key
		offset += 4;
	}
	else // TypeVarChar
	{
		int varLength = *(int *)key;
		memcpy((char *)curPageData + offset, key, sizeof(int) + varLength);
		// Set entry Offset = offset, and ptrFreeSpace, slotCount
		memcpy((char *)curPageData + PAGE_SIZE - 4*sizeof(int), &offset, sizeof(int)); // Offset
		memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &m, sizeof(int)); // slotCount
		offset += (4 + varLength);
		int ptrFreeSpace = offset + 4;
		memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int)); // ptrFS
	}
	*(int *)((char *)curPageData + offset) = p1; // Write p1
	ixfileHandle.fileHandle.appendPage(curPageData);

	// Update root id at page[0] + 4092
	rootId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
	ixfileHandle.fileHandle.readPage(0, curPageData);
	*(int *)((char *)curPageData + PAGE_SIZE - sizeof(int)) = rootId;
	ixfileHandle.fileHandle.writePage(0, curPageData);
	free(curPageData);
}

template<class T>
void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, T &key, const RID &rid, T &newEntry_key, int &newEntry_pageId)//PageIdIntIndex &newEntry)
{
	void *curPageData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, curPageData);
	bool isLeafNode = *(bool *)((char *)curPageData);
	int m = *(int *)((char *)curPageData + 1);
	if (!isLeafNode) // Index
	{
		int keyId = searchKeyInIndex(curPageData, m, key);
		int childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
		assert(childPageId < ixfileHandle.fileHandle.getNumberOfPages() && "insertKey() invalid childPageId!");
		if (childPageId >= 0)
		{
			insertKey(ixfileHandle, attribute, childPageId, key, rid, newEntry_key, newEntry_pageId); // Recursive call
			// If split happened in last recursion, add key index or push up it
			if (newEntry_pageId >= 0)
			{
				// flag, m, p0, k1, p1, k2, p2, k3, p3... {5, 9, 13, 17, ||} + 19, keyId = 4, m = 4
				keyId = searchKeyInIndex(curPageData, m, newEntry_key); // <= key
				int offset = 1 + (2*keyId+2)*sizeof(int);
				// Whether full or not, insert <key, pageId> to page
				bool isLastValidEntry = (keyId == m);
				if (!isLastValidEntry) // Index has no entry offsets, ptrFreeSpace, slotCount for TypeInt
					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
				memcpy((char *)curPageData + offset, &newEntry_key, sizeof(int));
				memcpy((char *)curPageData + offset + 4, &newEntry_pageId, sizeof(int));
				bool isPageFull = 1 + (2*m + 4)*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
				if (!isPageFull) // Leaf is not full
				{
					m++;
					*(int *)((char *)curPageData + 1) = m;
					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
					if (isShowIndex)
						cout << "Inserting index (" << newEntry_key << ", " << newEntry_pageId << ") at index[" << pageId << "], offset = " << offset << ", m = " << m << endl;
					newEntry_pageId = -1; // Split does not happen
				}
				else // No space, push up key (without pageId)
				{
					// The first d entries [0, d-1]stay
					int m1 = (m + 1)/2; // Inserted length is 8 bytes, since we will split, we do not worry about the overwritten 8 bytes of slot dir
					// Update m
					*(int *)((char *)curPageData + 1) = m;
					ixfileHandle.fileHandle.writePage(pageId, curPageData);

					//  Move the rest (m-d) entries [d+1, m] to new NL and push up [d]
					void *newPageData = malloc(PAGE_SIZE);
					isLeafNode = false;
					int newNonLeafId;
					int m_new = m - m1;
					memcpy((char *)newPageData, &isLeafNode, 1);
					memcpy((char *)newPageData + 1, &m_new, sizeof(int));
					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m1+3)*sizeof(int), (2*m_new+1)*sizeof(int)); // from p3
					ixfileHandle.fileHandle.appendPage(newPageData);

					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
					// Push up new entry to index
					// ex:After inserting <k2, p2>, index = {p0, k1, p1, k2, p2, k3, p3, k4, p4, k5, p5, k6, p6}
					// suppose m = 5 before <k2, p2> is inserted
					// When split happens, m1 = (5+1)/2 = 3, m_new = 5 - 3 = 2
					// After split, index_1 = {p0, k1, p1, k2, p2, k3, p3}, index_2 = {p4, k5, p5, k6, p6}, push up <k4, index_2_id>
					memcpy(&newEntry_key, (char *)curPageData + 1 + (2*m1+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
					newEntry_pageId = newNonLeafId; // ex: rChild = 7

					// If root page splits, set new id to root
					if (pageId == getRootId(ixfileHandle))
					{
						cout << "Split root, new root id[" << newNonLeafId << "], root_key = " << newEntry_key << endl;;
						setRootId(ixfileHandle, attribute, pageId, &newEntry_key, newEntry_pageId); // p0, 13, 7
						newEntry_pageId = -1; // End split
					}
					free(newPageData);
					if (isShowIndex)
						cout << "Split index page[" << pageId << "], push up (" << newEntry_key << ", " << newNonLeafId << ")\n";
				}
			}
		}
	}
	else // Leaf
	{
		// flag, m, prev, next, k1, r1, k2, r2...
		int entryOffset; // entry offset value in slot directory, not of the inserted entry!
		int ptrFreeSpace = *(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int));
		int slotCount = *(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int));
		// Remove all the invalid entryOffsets first
		removeEmptySlots(curPageData, m, slotCount);
//		if (m < slotCount)
//		{
//			for (int i = 0; i < slotCount; i++)
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int));
//				if (entryOffset < 0)
//				{
//					if (i < slotCount - 1)
//					{
//						int slotDirOffset = PAGE_SIZE - (slotCount + 3)*sizeof(int);
//						memmove((char *)curPageData + slotDirOffset + 4, (char *)curPageData + slotDirOffset, (slotCount - i-1)*sizeof(int));
//					}
//					slotCount --;
//					if (m == slotCount)
//						break;
//				}
//			}
//			// Compress slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//		}

		// If all elements in slot directory are -1, then slotCount = 0;
		// Perform binary search for fixed-length <key, rid> entry
		// Attention: returned keyId is the index of inserted valid entry
		int keyId = searchKeyInLeaf(curPageData, m, key); // {70, 80, 90, || 100}, insert 95, returned key id = 3
		int offset = 1 + (3*keyId+3)*sizeof(int);
		if (isShowLeaf)
		{
			cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << endl;
			cout << "In leaf Write key = " << key << " at offset = " << offset << "\n";
		}
		int insEntryLength = 3*sizeof(int); // Inserted entry length
		bool isLastValidEntry = (keyId == m);
		if (!isLastValidEntry)
		{
			int movedEntriesLength = ptrFreeSpace - offset - insEntryLength;
			memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
			// Update its following valid entries' offset
			for (int i = slotCount; i >= keyId + 1; i--) // Read from array[slotCount - 1]
			{
				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i + 3)*sizeof(int), sizeof(int));
				entryOffset += insEntryLength;
				memcpy((char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
			}
		}
		// Insert this entry first, since the length of inserted entry is 12 bytes for int/float type
		// If leaf is full after inserting, ignore the overwritten slot directory and split, create new slot dir for new leaf
		memcpy((char *)curPageData + offset, &key, sizeof(int));
		memcpy((char *)curPageData + offset + 4, &rid.pageNum, sizeof(int));
		memcpy((char *)curPageData + offset + 4*2, &rid.slotNum, sizeof(int));
		bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
		if (!isPageFull) // Leaf is not full
		{
			// Write inserted entryOffset
			memcpy((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int), &offset, sizeof(int));
			slotCount ++; // Update slotCount
			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
			m++; // Update number of valid data entries (m)
			memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
			// Update ptrFreeSpace
			ptrFreeSpace += insEntryLength;
			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
			ixfileHandle.fileHandle.writePage(pageId, curPageData);
			newEntry_pageId = -1;
		}
		else // Leaf is full, split
		{
			// The first d entries [0, d-1]stay
			int d = (m + 1)/2;
			// Update m
			memcpy((char *)curPageData + 1, &d, sizeof(int));
			// Update ptrFreeSpace
			ptrFreeSpace = 1 + (3*d+3)*sizeof(int);
			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
			slotCount = d; // Update slotCount
			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));

			//  Move the rest (m-d+1) entries [d, m] to new leaf and set it as child entry
			void *newPageData = malloc(PAGE_SIZE);
			isLeafNode = true;
			int m_new = m - d + 1;
			int prev = pageId; // New leaf's prev = curPageId
			int next;
			int newLeafId;
			memcpy(&next, (char *)curPageData + 1 + 2*sizeof(int), sizeof(int)); // Cur leaf's next pageId
			memcpy((char *)newPageData, &isLeafNode, 1);
			memcpy((char *)newPageData + 1, &m_new, sizeof(int));
			memcpy((char *)newPageData + 1 + sizeof(int), &prev, sizeof(int));
			memcpy((char *)newPageData + 1 + 2*sizeof(int), &next, sizeof(int));
			// Copy m_new data entries
			memcpy((char *)newPageData + 1 + 3*sizeof(int), (char *)curPageData + 1 + (3*d+3)*sizeof(int), 3*m_new*sizeof(int));
			// Update slotCount, ptrFreeSpace
			slotCount = m_new;
			memcpy((char *)newPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
			ptrFreeSpace = 1 + (3*m_new + 3)*sizeof(int);
			memcpy((char *)newPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
			// Create new slot directory
			for (int i = 0; i < m_new; i++)
			{
				entryOffset = 1 + (3*i+3)*sizeof(int);
				memcpy((char *)newPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), &entryOffset, sizeof(int));
			}
			ixfileHandle.fileHandle.appendPage(newPageData);

			// Update curPageId's next pointer
			newLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
			memcpy((char *)curPageData + 1 + 2*sizeof(int), &newLeafId, sizeof(int));
			ixfileHandle.fileHandle.writePage(pageId, curPageData);

			// If curLeaf has next leaf, update prev pointer of L's next leaf
			if (next >= 0)
			{
				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
				memcpy((char *)curPageData + 1 + sizeof(int), &newLeafId, sizeof(int));
				ixfileHandle.fileHandle.writePage(next, curPageData);
			}

			// Copy up new entry up to index
			memcpy(&newEntry_key, (char *)newPageData + 1 + 3*sizeof(int), sizeof(int));
			newEntry_pageId = newLeafId;

			// If node resides in leaf page, create new root
			if (pageId == getRootId(ixfileHandle))
			{
				setRootId(ixfileHandle, attribute, pageId, &newEntry_key, newEntry_pageId); // p0, 13, 7
				if (isShowLeaf)
					cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << newEntry_key << endl;
				newEntry_pageId = -1; // End split
			}
			free(newPageData);
			if (isShowLeaf)
			{
				cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newLeafId << "], ";
				cout << "Copy up new entry (" << newEntry_key << ", " << newLeafId << ") to index!\n";
			}
		}
	}
	free(curPageData);
}

void insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, char *key, const RID &rid, char *newEntry_key, int &newEntry_pageId)
{
	void *curPageData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, curPageData);
	int m = *(int *)((char *)curPageData + 1);
	bool isLeafNode = *(bool *)((char *)curPageData);

	if (!isLeafNode) // Index
	{
		int keyId = searchKeyInIndex(curPageData, m, key, attribute);
		assert(keyId >= 0 && keyId <= m && "insertKey(char *): searchKeyInIndex() failed!\n");
		int childPageId;
		if (keyId == 0)
			childPageId = *(int *)((char *)curPageData + 5);
		else
		{
			int tmpOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId + 3)*4);
			int len = *(int *)((char *)curPageData + tmpOffset);
			childPageId = *(int *)((char *)curPageData + tmpOffset + 4 + len);
		}
		assert(childPageId < ixfileHandle.fileHandle.getNumberOfPages() && childPageId >= 0 && "insertKey(char *): Invalid child page id returned!\n");
		if (childPageId >= 0)
		{
			insertKey(ixfileHandle, attribute, childPageId, key, rid, newEntry_key, newEntry_pageId); // Recursive call
			if (newEntry_pageId >= 0) // If split happened in last recursion, add key index or push up it
			{
				keyId = searchKeyInIndex(curPageData, m, newEntry_key, attribute);
//				cout << "returned kId in indx: " << keyId << "\n";
				assert(keyId <= m && keyId >= 0 && "insertKey(char *): searchKeyInIndex() #2 failed!\n");

				int ptrFreeSpace = *(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int));
				int slotCount = *(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int));
				int keyId = searchKeyInIndex(curPageData, m, newEntry_key, attribute);
				assert(keyId <= m && keyId >= 0 && "insertKey(char *): searchKeyInLeaf() failed!\n");
				bool isLastValidEntry = (keyId == m);
				int offset;
				if (isLastValidEntry)
					offset = ptrFreeSpace; // This key shoudl be inserted at the end of ptrFreeSpace
				else
					offset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int)); // kI = 0, go to last 4th int
				if (isShowIndex)
				{
					cout << "Insert to index[" << pageId << "], m = " << m << ", keyId = " << keyId
							<< ", key = " << *(newEntry_key+4) << " at offset = " << offset << "\n";
				}
				int varLength = *(int *)newEntry_key;
				int insEntryLength = (2*4 + varLength); // Inserted entry length
				bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4 + insEntryLength > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
				if (!isPageFull) // Index is not full
				{
					if (!isLastValidEntry)
					{
						int movedEntriesLength = ptrFreeSpace - offset;
						memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
						// Update its following valid entries' offset
						for (int i = slotCount - 1; i >= keyId; i--) // Read from array[slotCount - 1]
						{
							int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int));
							entryOffset += insEntryLength;
							*(int *)((char *)curPageData + PAGE_SIZE - (i+2 + 3)*sizeof(int)) = entryOffset;
						}
					}
					memcpy((char *)curPageData + offset, newEntry_key, 4 + varLength);
					memcpy((char *)curPageData + offset + 4 + varLength, &newEntry_pageId, sizeof(int));
//					cout << "newE_pId = " << newEntry_pageId << endl;
					// Write inserted entryOffset
					*(int *)((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int)) = offset;
					slotCount ++; // Update slotCount
					*(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int)) = slotCount;
					m++; // Update number of valid data entries (m)
					*(int *)((char *)curPageData + 1) = m;
					// Update ptrFreeSpace
					ptrFreeSpace += insEntryLength;
					*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
					ixfileHandle.fileHandle.writePage(pageId, curPageData);
					if (isShowIndex)
						cout << "Inserting index (" << *(newEntry_key+4) << *(int *)newEntry_key << ", " << newEntry_pageId << ") at index[" << pageId << "], offset = " << offset << ", m = " << m << endl;
					newEntry_pageId = -1;
				}
				else // Page is full, split, push up key (without pageId)
				{
					int m1; // Number of entries that stay in the same page to maintain 50% occupancy
					get_m1_InPage1(curPageData, keyId, insEntryLength, m1);
					void *newPageData = malloc(PAGE_SIZE);
					*(int *)((char *)curPageData + 1) = m1; // Update m1
					slotCount = m1;
					*(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int)) = m1; // Update slotCount1
					if (isShowIndex)
						cout << "After split, index[" << pageId << "], m1 = " << m1 << "\n";

					createNewIndex(curPageData, m, m1, pageId, keyId, insEntryLength, newEntry_key, newEntry_pageId, newPageData, attribute);
					if (keyId < m1) // Insert key to index_1
					{
						int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 3)*sizeof(int)); // entryOffset of key[84]
						ptrFreeSpace = entryOffset + insEntryLength; // Update ptrFreeSpace
						*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
						if (keyId != m1 - 1) // Update slot dir
						{
							entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int)); // eO of key[id], eg, key[65]
							// Attention: ptrFreeSpace is already updated, movedLength = ptrFS - enOf - (4+varLen)
							memmove((char *)curPageData + entryOffset + insEntryLength, (char *)curPageData + entryOffset, ptrFreeSpace - entryOffset - insEntryLength);
							for (int i = slotCount - 2; i >= keyId; i--) // Now m1 = 85, then before key is inserted, m1 = 84, [60] to [83] ->
							{
								memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int)); //[83]
								entryOffset += insEntryLength;
								memcpy((char *)curPageData + PAGE_SIZE - (i+2 + 3)*sizeof(int), &entryOffset, sizeof(int));// [84]
							}
						}
						// Write entry to leaf_1
						entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int));
						varLength = *(int *)newEntry_key;
						cout << "Inserting key[" << keyId << "] to index[" << pageId << "], " << *(newEntry_key+4) << *(int *)newEntry_key << ", enOffset = " << entryOffset << "\n";
						memcpy((char *)curPageData + entryOffset, newEntry_key, 4 + varLength);
						memcpy((char *)curPageData + entryOffset + 4 + varLength, &newEntry_pageId, sizeof(int));
					}
					else // Insert key to index_2, keyId >= m
					{
						ptrFreeSpace = *(int *)((char *)curPageData + PAGE_SIZE - (m1+1 + 3)*sizeof(int)); // Update ptrFreeSpace
						*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
					}

					ixfileHandle.fileHandle.appendPage(newPageData);
					int newPageId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
					*(int *)((char *)curPageData + 1 + 2*sizeof(int)) = newPageId;
					ixfileHandle.fileHandle.writePage(pageId, curPageData);
					// Push up new entry up to index
					int offset1 = *(int *)((char *)curPageData + PAGE_SIZE - (m1+1 + 3)*4);
					varLength = *(int *)((char *)curPageData + offset1);
					memcpy(newEntry_key, (char *)curPageData + offset1, 4 + varLength);
					newEntry_pageId = newPageId;

					// If root page splits, set new id to root
					if (pageId == getRootId(ixfileHandle))
					{
						cout << "Split root, new root id[" << newPageId << "], root_key = " << newEntry_key << endl;;
						setRootId(ixfileHandle, attribute, pageId, newEntry_key, newEntry_pageId); // p0, 13, 7
						newEntry_pageId = -1; // End split
					}
					free(newPageData);
					cout << "Split index page[" << pageId << "], push up (" << *(newEntry_key + 4) << ", " << newPageId << ")\n";
					if (isShowIndex)
					{
						cout << "Index[" << pageId << "] has no space, add new leaf[" << newPageId << "], ";
						cout << "Copy up new entry (" << *(newEntry_key + 4) << ", " << newPageId << ") to upper index!\n";
					}
				}
			}
		}
	}
	else // Leaf
	{
		// flag, m, prev, next, k1, r1, k2, r2...
		int ptrFreeSpace = *(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int));
		int slotCount = *(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int));
		// Remove all the invalid entryOffsets first
		removeEmptySlots(curPageData, m, slotCount);
		int keyId = searchKeyInLeaf(curPageData, m, key, attribute);
		assert(keyId <= m && keyId >= 0 && "insertKey(char *): searchKeyInLeaf() failed!\n");
		int offset; // Offset of where the inserted entry should be stored
		bool isLastValidEntry = (keyId == m);
		if (isLastValidEntry)
			offset = ptrFreeSpace; // This key shoudl be inserted at the end of ptrFreeSpace
		else
			offset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int)); // kI = 0, go to last 4th int
		if (isShowLeaf)
		{
			cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << ", key = " << *(char *)(key + 4) << "(" << *(int *)key << ")"
					<< " at offset = " << offset
					<< ", SC = " << slotCount << "\n";
		}
		int varLength = *(int *)key;
		int insEntryLength = varLength + 3*sizeof(int); // Inserted entry length (len, data, rid)
		bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4 + insEntryLength > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
		if (!isPageFull) // Leaf is not full
		{
			if (!isLastValidEntry)
			{
				int movedEntriesLength = ptrFreeSpace - offset;
				memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
				// Update its following valid entries' offset
				for (int i = slotCount - 1; i >= keyId; i--) // Read from array[slotCount - 1]
				{
					int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int));
					entryOffset += insEntryLength;
					int len = *(int *)((char *)curPageData + entryOffset);
					if (len > attribute.length)
					{
						cout << "[" << pageId << "], i = " << i << ", m = " << m << ", kId = " << keyId << endl;
						assert( len <= attribute.length && "update slot dir error!");
					}
					*(int *)((char *)curPageData + PAGE_SIZE - (i+2 + 3)*sizeof(int)) = entryOffset;
				}
			}
//			if (pageId == 1 && m == 60)
//			{
//				int eO = *(int *)((char *)curPageData + PAGE_SIZE - ((m+1) + 3)*sizeof(int));
//				cout << "Inserted key[" << rid.pageNum << "] is the last valid entry in page[" << pageId
//						<< "], I_len = " << varLength << ", kId = " << keyId << ", L.len = "
//						<< *(int *)((char *)curPageData + eO)  << "\n";
//				assert( pageId != 1 && "pageId = 1");
//			}
			memcpy((char *)curPageData + offset, key, 4 + varLength);
			memcpy((char *)curPageData + offset + 4 + varLength, &rid.pageNum, sizeof(int));
			memcpy((char *)curPageData + offset + 2*4 + varLength, &rid.slotNum, sizeof(int));
			// Write inserted entryOffset
			*(int *)((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int)) = offset;
			slotCount ++; // Update slotCount
			*(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int)) = slotCount;
			m++; // Update number of valid data entries (m)
			*(int *)((char *)curPageData + 1) = m;
			// Update ptrFreeSpace
			ptrFreeSpace += insEntryLength;
			*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
			ixfileHandle.fileHandle.writePage(pageId, curPageData);
			newEntry_pageId = -1;
		}
		else // Leaf is full, split
		{
			int m1; // Number of entries that stay in the same page to maintain 50% occupancy
			get_m1_InPage1(curPageData, keyId, insEntryLength, m1);
			void *newPageData = malloc(PAGE_SIZE);
			*(int *)((char *)curPageData + 1) = m1; // Update m1
			slotCount = m1;
			*(int *)((char *)curPageData + PAGE_SIZE - 3*sizeof(int)) = m1; // Update slotCount1
//			cout << "After split, leaf[" << pageId << "], m1 = " << m1 << "\n";

			// Prepare data for new leaf
			RC rc = createNewLeaf(curPageData, m, m1, pageId, keyId, insEntryLength, key, rid, newPageData, attribute);
			assert(rc == 0 && "createNewLeaf() failed.");
			if (keyId < m1) // Insert key to leaf_1
			{
				 // m1 = 85, kId = 65, now only 84 entries inside b4 key_inserted, get offset[84],
				int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (m1 + 3)*sizeof(int)); // entryOffset of key[84]
				ptrFreeSpace = entryOffset + insEntryLength; // Update ptrFreeSpace
				*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
				if (keyId != m1 - 1) // Update slot dir
				{
					entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int)); // eO of key[id], eg, key[65]
					// Attention: ptrFreeSpace is already updated, movedLength = ptrFS - enOf - (4+varLen)
					memmove((char *)curPageData + entryOffset + insEntryLength, (char *)curPageData + entryOffset, ptrFreeSpace - entryOffset - insEntryLength);
//					cout << "Move [" << keyId << "] - [" << slotCount - 2 << "], insert [" << keyId << "]\n";
					for (int i = slotCount - 2; i >= keyId; i--) // Now m1 = 85, then before key is inserted, m1 = 84, [60] to [83] ->
					{
						memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int)); //[83]
						entryOffset += insEntryLength;
						int len = *(int *)((char *)curPageData + entryOffset);
						if (len > attribute.length)
						{
							cout << "Leaf [" << pageId << "], i = " << i << ", kId = " << keyId << endl;
							assert(len <= attribute.length && "leal full, ist key to old leaf failed!");
						}
						memcpy((char *)curPageData + PAGE_SIZE - (i+2 + 3)*sizeof(int), &entryOffset, sizeof(int));// [84]
					}
				}
				// Write key and rid to leaf_1
				entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int));
				varLength = *(int *)key;
//				cout << "Inserting key[" << keyId << "] to leaf[" << pageId << "], " << *(key+4) << *(int *)key << ", enOffset = " << entryOffset << "\n";
				memcpy((char *)curPageData + entryOffset, key, 4 + varLength);
				memcpy((char *)curPageData + entryOffset + 4 + varLength, &rid.pageNum, sizeof(int));
				memcpy((char *)curPageData + entryOffset + 2*4 + varLength, &rid.slotNum, sizeof(int));
			}
			else // Insert key to leaf_2, keyId >= m1
			{
				// m1 = 60, [0]-[59], ptrFS = offset[60]
				ptrFreeSpace = *(int *)((char *)curPageData + PAGE_SIZE - (m1 +1+ 3)*sizeof(int)); // Update ptrFreeSpace
				assert(ptrFreeSpace < PAGE_SIZE && "create new leaf, ist key to new leaf error!");
				*(int *)((char *)curPageData + PAGE_SIZE - 2*sizeof(int)) = ptrFreeSpace;
			}

			ixfileHandle.fileHandle.appendPage(newPageData);
			int next = *(int *)((char *)curPageData + 1 + 2*sizeof(int)); // Cur leaf's next pageId
			// Update curPageId's next pointer
			int newPageId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
			*(int *)((char *)curPageData + 1 + 2*sizeof(int)) = newPageId;
			ixfileHandle.fileHandle.writePage(pageId, curPageData);
			// If curLeaf has next leaf, update prev pointer of L's next leaf
			if (next >= 0)
			{
				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
				*(int *)((char *)curPageData + 1 + sizeof(int)) = newPageId;
				ixfileHandle.fileHandle.writePage(next, curPageData);
			}
			// Copy up new entry up to index
			varLength = *(int *)((char *)newPageData + 1 + 3*sizeof(int));
			assert(varLength <= attribute.length && "insertKey() invalid varLength\n.");
			memcpy(newEntry_key, (char *)newPageData + 1 + 3*sizeof(int), 4 + varLength);
			newEntry_pageId = newPageId;

			// If node resides in leaf page, create new root
			if (pageId == getRootId(ixfileHandle))
			{
				setRootId(ixfileHandle, attribute, pageId, newEntry_key, newEntry_pageId); // p0, 13, 7
				if (isShowLeaf)
					cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << *(newEntry_key + 4) << *(int*)newEntry_key << endl;
				newEntry_pageId = -1; // End split
			}
			free(newPageData);
			if (isShowLeaf)
			{
				cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newPageId << "], ";
				cout << "Copy up new entry (" << *(newEntry_key + 4) << *(int*)newEntry_key << ", " << newPageId << ") to index!\n";
			}
		}
	}
	free(curPageData);
}


//void IndexManager::insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, int &key, const RID &rid, IntIndex &newEntry)
//{
//	int m;
//	bool isLeafNode;
//	void *curPageData = malloc(PAGE_SIZE);
//
//	ixfileHandle.fileHandle.readPage(pageId, curPageData);
//	memcpy(&isLeafNode, (char *)curPageData, 1);
//	memcpy(&m, (char *)curPageData + 1, sizeof(int));
//
//	if (!isLeafNode) // Non-leaf
//	{
//		int keyId = searchKeyInIndex(curPageData, m, key);
//		int childPageId;
//		memcpy(&childPageId, (char *)curPageData + 1 + (2*keyId+1)*sizeof(int), sizeof(int));
//		if (childPageId >= 0)
//		{
//			insertKey(ixfileHandle, attribute, childPageId, key, rid, newEntry); // Recursive call
//			// If split happened in last recursion, add key index or push up it
//			if (newEntry.pageId >= 0)
//			{
//				// flag, m, p0, k1, p1, k2, p2, k3, p3... {5, 9, 13, 17, ||} + 19, keyId = 4, m = 4
//				keyId = searchKeyInIndex(curPageData, m, key); // <= key
//				int offset;
//				offset = 1 + (2*keyId+2)*sizeof(int);
//				// Whether full or not, insert <key, pageId> to page
//				//				int insEntryLength = 2*sizeof(int); // Inserted entry length
//				bool isLastValidEntry = (keyId == m);
//				if (!isLastValidEntry) // Index has no entry offsets, ptrFreeSpace, slotCount for TypeInt
//				{
//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				}
//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				memcpy((char *)curPageData + offset + 4, &newEntry.pageId, sizeof(int));
//				bool isPageFull = 1 + (2*m + 4)*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
//				if (!isPageFull) // Leaf is not full
//				{
//					m++;
//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//					newEntry.pageId = -1; // Split does not happen
//				}
//
//				//				if (keyId < m) // 0 <= keyId <= m
//				//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				//				offset += 4;
//				//				memcpy((char *)curPageData + offset, &newEntry.pageId, sizeof(int));
//				//				bool isPageFull = 3*keyId
//				//				if (m < 2*d) // NL has space
//				//				{
//				//					//					cout << "NL[" << pageId << "] has space for the pushing/copying up key\n";
//				//					m++;
//				//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//				//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//				//					newEntry.pageId = -1; // Split does not happen
//				//				}
//				else // No space, push up key (without pageId)
//				{
//					// The first d entries [0, d-1]stay
//					int m1 = (m + 1)/2; // Inserted length is 8 bytes, since we will split, we do not worry about the overwritten 8 bytes of slot dir
//					// Update m
//					memcpy((char *)curPageData + 1, &m1, sizeof(int));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest (m-d) entries [d+1, m] to new NL and push up [d]
//					void *newPageData = malloc(PAGE_SIZE);
//					isLeafNode = false;
//					int newNonLeafId;
//					int m_new = m - m1;
//					memcpy((char *)newPageData, &isLeafNode, 1);
//					memcpy((char *)newPageData + 1, &m_new, sizeof(int));
//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m1+3)*sizeof(int), (2*m_new+1)*sizeof(int)); // from p3
//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// The first d entries (key_1 to key_d) stay
//					//					m = d;
//					//					memcpy((char *)curPageData + 1, &m, sizeof(int));
//					//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest d entries (k_{d+2} to k_{2d+1}) to new NL, push up key_{d+1}
//					//					void *newPageData = malloc(PAGE_SIZE);
//					//					isLeafNode = false;
//					//					int newNonLeafId;
//					//					memcpy((char *)newPageData, &isLeafNode, 1); // false
//					//					memcpy((char *)newPageData + 1, &m, sizeof(int)); // m = d
//					//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m+3)*sizeof(int), (2*m+1)*sizeof(int)); // from p3
//					//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// Push up new entry to index
//					//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					// ex:After inserting <k2, p2>, index = {p0, k1, p1, k2, p2, k3, p3, k4, p4, k5, p5, k6, p6}
//					// suppose m = 5 before <k2, p2> is inserted
//					// When split happens, m1 = (5+1)/2 = 3, m_new = 5 - 3 = 2
//					// After split, index_1 = {p0, k1, p1, k2, p2, k3, p3}, index_2 = {p4, k5, p5, k6, p6}, push up <k4, index_2_id>
//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m1+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					newEntry.pageId = newNonLeafId; // ex: rChild = 7
//
//					// If root page splits, set new id to root
//					if (pageId == getRootId(ixfileHandle))
//					{
//						cout << "Split root, new root id[" << newNonLeafId << "], root_key = " << newEntry.key << endl;;
//						setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//						newEntry.pageId = -1; // End split
//					}
//					free(newPageData);
//					cout << "Split index page[" << pageId << "], push up (" << newEntry.key << ", " << newNonLeafId << ")\n";
//				}
//			}
//		}
//	}
//	else // Leaf
//	{
//		// flag, m, prev, next, k1, r1, k2, r2...
//		memcpy(&m, (char *)curPageData + 1, sizeof(unsigned)); // get number of valid data entries
//		int ptrFreeSpace;
//		int slotCount;
//		int entryOffset; // entry offset value in slot directory, not of the inserted entry!
//		memcpy(&ptrFreeSpace, (char *)curPageData + PAGE_SIZE - 2*sizeof(int), sizeof(int));
//		memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
//		// Remove all the invalid entryOffsets first
//		if (m < slotCount)
//		{
//			for (int i = 0; i < slotCount; i++)
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int));
//				if (entryOffset < 0)
//				{
//					if (i < slotCount - 1)
//					{
//						int slotDirOffset = PAGE_SIZE - (slotCount + 3)*sizeof(int);
//						memmove((char *)curPageData + slotDirOffset + 4, (char *)curPageData + slotDirOffset, (slotCount - i-1)*sizeof(int));
//					}
//					slotCount --;
//					if (m == slotCount)
//						break;
//				}
//			}
//			// Compress slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//		}
//		// If all elements in slot directory are -1, then slotCount = 0;
//		// Perform binary search for fixed-length <key, rid> entry
//		// Attention: returned keyId is the index of inserted valid entry
//		int keyId = searchKeyInLeaf(curPageData, m, key); // {70, 80, 90, || 100}, insert 95, returned key id = 3
//		int offset = 1 + (3*keyId+3)*sizeof(int);
//		//		cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << endl;
//		//				cout << "In leaf Write key = " << key << " at offset = " << offset << "\n";
//
//		int insEntryLength = 3*sizeof(int); // Inserted entry length
//		bool isLastValidEntry = (keyId == m);
//		if (!isLastValidEntry) //
//		{
//			int movedEntriesLength = ptrFreeSpace - offset - insEntryLength;
//			memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
//			// Update its following valid entries' offset
//			for (int i = slotCount; i >= keyId + 1; i--) // Read from array[slotCount - 1]
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i + 3)*sizeof(int), sizeof(int));
//				entryOffset += insEntryLength;
//				memcpy((char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//		}
//		// Insert this entry first, since the length of inserted entry is 12 bytes for int/float type
//		// If leaf is full after inserting, ignore the overwritten slot directory and split, create new slot dir for new leaf
//		memcpy((char *)curPageData + offset, &key, sizeof(int));
//		memcpy((char *)curPageData + offset + 4, &rid.pageNum, sizeof(int));
//		memcpy((char *)curPageData + offset + 4*2, &rid.slotNum, sizeof(int));
//		bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
//		if (!isPageFull) // Leaf is not full
//		{
//			// Write inserted entryOffset
//			memcpy((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int), &offset, sizeof(int));
//			slotCount ++; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			m++; // Update number of valid data entries (m)
//			memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//			// Update ptrFreeSpace
//			ptrFreeSpace += insEntryLength;
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//			newEntry.pageId = -1;
//		}
//		else // Leaf is full, split
//		{
//			// The first d entries [0, d-1]stay
//			int d = (m + 1)/2;
//			// Update m
//			memcpy((char *)curPageData + 1, &d, sizeof(int));
//			// Update ptrFreeSpace
//			ptrFreeSpace = 1 + (3*d+3)*sizeof(int);
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			slotCount = d; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//
//			//  Move the rest (m-d+1) entries [d, m] to new leaf and set it as child entry
//			void *newPageData = malloc(PAGE_SIZE);
//			isLeafNode = true;
//			int m_new = m - d + 1;
//			int prev = pageId; // New leaf's prev = curPageId
//			int next;
//			int newLeafId;
//			memcpy(&next, (char *)curPageData + 1 + 2*sizeof(int), sizeof(int)); // Cur leaf's next pageId
//			memcpy((char *)newPageData, &isLeafNode, 1);
//			memcpy((char *)newPageData + 1, &m_new, sizeof(int));
//			memcpy((char *)newPageData + 1 + sizeof(int), &prev, sizeof(int));
//			memcpy((char *)newPageData + 1 + 2*sizeof(int), &next, sizeof(int));
//			// Copy m_new data entries
//			memcpy((char *)newPageData + 1 + 3*sizeof(int), (char *)curPageData + 1 + (3*d+3)*sizeof(int), 3*m_new*sizeof(int));
//			// Update slotCount, ptrFreeSpace
//			slotCount = m_new;
//			memcpy((char *)newPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			ptrFreeSpace = 1 + (3*m_new + 3)*sizeof(int);
//			memcpy((char *)newPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			// Create new slot directory
//			for (int i = 0; i < m_new; i++)
//			{
//				entryOffset = 1 + (3*i+3)*sizeof(int);
//				memcpy((char *)newPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//			ixfileHandle.fileHandle.appendPage(newPageData);
//
//			// Update curPageId's next pointer
//			newLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//			memcpy((char *)curPageData + 1 + 2*sizeof(int), &newLeafId, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//			// If curLeaf has next leaf, update prev pointer of L's next leaf
//			if (next >= 0)
//			{
//				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
//				memcpy((char *)curPageData + 1 + sizeof(int), &newLeafId, sizeof(int));
//				ixfileHandle.fileHandle.writePage(next, curPageData);
//			}
//
//			// Copy up new entry up to index
//			memcpy(&newEntry.key, (char *)newPageData + 1 + 3*sizeof(int), sizeof(int));
//			newEntry.pageId = newLeafId;
//
//			// If node resides in leaf page, create new root
//			if (pageId == getRootId(ixfileHandle))
//			{
//				setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//				cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << newEntry.key << endl;
//				newEntry.pageId = -1; // End split
//			}
//			free(newPageData);
//			//			cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newLeafId << "], ";
//			//			cout << "Copy up new entry (" << newEntry.key << ", " << newLeafId << ") to index!\n";
//		}
//	}
//	free(curPageData);
//}
//
//void IndexManager::insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, float &key, const RID &rid, FloatIndex &newEntry)
//{
//	int m;
//	bool isLeafNode;
//	void *curPageData = malloc(PAGE_SIZE);
//
//	ixfileHandle.fileHandle.readPage(pageId, curPageData);
//	memcpy(&isLeafNode, (char *)curPageData, 1);
//	memcpy(&m, (char *)curPageData + 1, sizeof(int));
//
//	if (!isLeafNode) // Non-leaf
//	{
//		int keyId = searchKeyInIndex(curPageData, m, key);
//		int childPageId;
//		memcpy(&childPageId, (char *)curPageData + 1 + (2*keyId+1)*sizeof(int), sizeof(int));
//		if (childPageId >= 0)
//		{
//			//			cout << "In index[" << childPageId << "]: \n";
//			insertKey(ixfileHandle, attribute, childPageId, key, rid, newEntry); // Recursive call
//			// If split happened in last recursion, add key index or push up it
//			if (newEntry.pageId >= 0)
//			{
//				// flag, m, p0, k1, p1, k2, p2, k3, p3... {5, 9, 13, 17, ||} + 19, keyId = 4, m = 4
//				keyId = searchKeyInIndex(curPageData, m, key); // <= key
//				int offset;
//				offset = 1 + (2*keyId+2)*sizeof(int);
//				if (keyId < m) // 0 <= keyId <= m
//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				offset += 4;
//				memcpy((char *)curPageData + offset, &newEntry.pageId, sizeof(int));
//				//				bool isPageFull = 3*keyId
//				if (m < 2*d) // NL has space
//				{
//					//					cout << "NL[" << pageId << "] has space for the pushing/copying up key\n";
//					m++;
//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//					newEntry.pageId = -1; // Split does not happen
//				}
//				else // No space, push up key (without pageId)
//				{
//					// The first d entries (key_1 to key_d) stay
//					m = d;
//					memcpy((char *)curPageData + 1, &m, sizeof(int));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest d entries (k_{d+2} to k_{2d+1}) to new NL, push up key_{d+1}
//					void *newPageData = malloc(PAGE_SIZE);
//					isLeafNode = false;
//					int newNonLeafId;
//					memcpy((char *)newPageData, &isLeafNode, 1); // false
//					memcpy((char *)newPageData + 1, &m, sizeof(int)); // m = d
//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m+3)*sizeof(int), (2*m+1)*sizeof(int)); // from p3
//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// Push up new entry to index
//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					newEntry.pageId = newNonLeafId; // ex: rChild = 7
//
//					// If root page splits, set new id to root
//					if (pageId == getRootId(ixfileHandle))
//					{
//						cout << "Split root, new root id[" << newNonLeafId << "], root_key = " << newEntry.key << endl;;
//						setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//						newEntry.pageId = -1; // End split
//					}
//					free(newPageData);
//					cout << "Split index page[" << pageId << "], push up (" << newEntry.key << ", " << newNonLeafId << ")\n";
//				}
//			}
//		}
//	}
//	else // Leaf
//	{
//		// flag, m, prev, next, k1, r1, k2, r2...
//		memcpy(&m, (char *)curPageData + 1, sizeof(unsigned)); // get number of valid data entries
//		int ptrFreeSpace;
//		int slotCount;
//		int entryOffset; // entry offset value in slot directory, not of the inserted entry!
//		memcpy(&ptrFreeSpace, (char *)curPageData + PAGE_SIZE - 2*sizeof(int), sizeof(int));
//		memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
//		// Remove all the invalid entryOffsets first
//		if (m < slotCount)
//		{
//			//			cout << "// Remove all the invalid entryOffsets first\n";
//			for (int i = 0; i < slotCount; i++)
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int));
//				if (entryOffset < 0)
//				{
//					if (i < slotCount - 1)
//					{
//						int slotDirOffset = PAGE_SIZE - (slotCount + 3)*sizeof(int);
//						memmove((char *)curPageData + slotDirOffset + 4, (char *)curPageData + slotDirOffset, (slotCount - i-1)*sizeof(int));
//					}
//					slotCount --;
//					if (m == slotCount)
//						break;
//				}
//			}
//			// Compress slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//		}
//		// If all elements in slot directory are -1, then slotCount = 0;
//		// Perform binary search for fixed-length <key, rid> entry
//		// Attention: returned keyId is the index of inserted valid entry
//		int keyId = searchKeyInLeaf(curPageData, m, key); // {70, 80, 90, || 100}, insert 95, returned key id = 3
//		int offset = 1 + (3*keyId+3)*sizeof(int);
//		//		cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << endl;
//		//				cout << "In leaf Write key = " << key << " at offset = " << offset << "\n";
//
//
//		int insEntryLength = 3*sizeof(int); // Inserted entry length
//		bool isLastValidEntry = (keyId == m);
//		if (!isLastValidEntry) //
//		{
//			int movedEntriesLength = ptrFreeSpace - offset - insEntryLength;
//			memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
//			// Update its following valid entries' offset
//			for (int i = slotCount; i >= keyId + 1; i--) // Read from array[slotCount - 1]
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i + 3)*sizeof(int), sizeof(int));
//				entryOffset += insEntryLength;
//				memcpy((char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//		}
//		// Insert this entry first, since the length of inserted entry is 12 bytes for int/float type
//		// If leaf is full after inserting, ignore the overwritten slot directory and split, create new slot dir for new leaf
//		memcpy((char *)curPageData + offset, &key, sizeof(int));
//		memcpy((char *)curPageData + offset + 4, &rid.pageNum, sizeof(int));
//		memcpy((char *)curPageData + offset + 4*2, &rid.slotNum, sizeof(int));
//		bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
//		if (!isPageFull) // Leaf is not full
//		{
//			// Write inserted entryOffset
//			memcpy((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int), &offset, sizeof(int));
//			slotCount ++; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			m++; // Update number of valid data entries (m)
//			memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//			// Update ptrFreeSpace
//			ptrFreeSpace += insEntryLength;
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//			newEntry.pageId = -1;
//		}
//		else // Leaf is full, split
//		{
//			// The first d entries [0, d-1]stay
//			int d = (m + 1)/2;
//			// Update m
//			memcpy((char *)curPageData + 1, &d, sizeof(int));
//			// Update ptrFreeSpace
//			ptrFreeSpace = 1 + (3*d+3)*sizeof(int);
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			slotCount = d; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//
//			//  Move the rest (m-d+1) entries [d, m] to new leaf and set it as child entry
//
//			void *newPageData = malloc(PAGE_SIZE);
//			isLeafNode = true;
//			int m_new = m - d + 1;
//			int prev = pageId; // New leaf's prev = curPageId
//			int next;
//			int newLeafId;
//			//			cout << "m1 = " << d << ", m2 = " << m_new << endl;
//			memcpy(&next, (char *)curPageData + 1 + 2*sizeof(int), sizeof(int)); // Cur leaf's next pageId
//			memcpy((char *)newPageData, &isLeafNode, 1);
//			memcpy((char *)newPageData + 1, &m_new, sizeof(int));
//			memcpy((char *)newPageData + 1 + sizeof(int), &prev, sizeof(int));
//			memcpy((char *)newPageData + 1 + 2*sizeof(int), &next, sizeof(int));
//			// Copy m_new data entries
//			memcpy((char *)newPageData + 1 + 3*sizeof(int), (char *)curPageData + 1 + (3*d+3)*sizeof(int), 3*m_new*sizeof(int));
//			// Update slotCount, ptrFreeSpace
//			slotCount = m_new;
//			memcpy((char *)newPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			ptrFreeSpace = 1 + (3*m_new + 3)*sizeof(int);
//			memcpy((char *)newPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			// Create new slot directory
//			for (int i = 0; i < m_new; i++)
//			{
//				entryOffset = 1 + (3*i+3)*sizeof(int);
//				memcpy((char *)newPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//			ixfileHandle.fileHandle.appendPage(newPageData);
//
//			// Update curPageId's next pointer
//			newLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//			memcpy((char *)curPageData + 1 + 2*sizeof(int), &newLeafId, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//			// If curLeaf has next leaf, update prev pointer of L's next leaf
//			if (next >= 0)
//			{
//				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
//				memcpy((char *)curPageData + 1 + sizeof(int), &newLeafId, sizeof(int));
//				ixfileHandle.fileHandle.writePage(next, curPageData);
//			}
//
//			// Copy up new entry up to index
//			memcpy(&newEntry.key, (char *)newPageData + 1 + 3*sizeof(int), sizeof(int));
//			newEntry.pageId = newLeafId;
//
//			// If node resides in leaf page, create new root
//			if (pageId == getRootId(ixfileHandle))
//			{
//				setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//				cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << newEntry.key << endl;
//				newEntry.pageId = -1; // End split
//			}
//			free(newPageData);
//			//			cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newLeafId << "], ";
//			//			cout << "Copy up new entry (" << newEntry.key << ", " << newLeafId << ") to index!\n";
//		}
//	}
//	free(curPageData);
//}

//void IndexManager::insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, char *key, const RID &rid, char *newKey, int &newPageId)
//{
//	int m;
//	bool isLeafNode;
//	void *curPageData = malloc(PAGE_SIZE);
//
//	ixfileHandle.fileHandle.readPage(pageId, curPageData);
//	memcpy(&isLeafNode, (char *)curPageData, 1);
//	memcpy(&m, (char *)curPageData + 1, sizeof(int));
//
//	if (!isLeafNode) // Non-leaf
//	{
//		int keyId = searchKeyInIndex(curPageData, m, key, attribute);
//		int childPageId;
//		memcpy(&childPageId, (char *)curPageData + 1 + (2*keyId+1)*sizeof(int), sizeof(int));
//		if (childPageId >= 0)
//		{
//			insertKey(ixfileHandle, attribute, childPageId, key, rid, newKey, newPageId); // Recursive call
//			// If split happened in last recursion, add key index or push up it
//			if (newPageId >= 0)
//			{
//				// flag, m, p0, k1, p1, k2, p2, k3, p3... {5, 9, 13, 17, ||} + 19, keyId = 4, m = 4
//				keyId = searchKeyInIndex(curPageData, m, key, attribute); // <= key
//				int offset;
//				//				offset = 1 + (2*keyId+2)*sizeof(int);
//				memcpy(&offset, (char *)curPageData + PAGE_SIZE - (keyId+1 + 3)*sizeof(int), sizeof(int));
//				int varLength;
//				memcpy(&varLength, newKey, sizeof(int));
//				memcpy((char *)curPageData + offset, newKey, 4 + varLength);
//				offset += (4 + varLength);
//				memcpy((char *)curPageData + offset, &newPageId, sizeof(int));
//				// Whether full or not, insert <key, pageId> to page
//				//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				//				offset += 4;
//				//				memcpy((char *)curPageData + offset, &newEntry.pageId, sizeof(int));
//				int insEntryLength = 4 + varLength; // Inserted entry length
//				bool isLastValidEntry = (keyId == m);
//				if (!isLastValidEntry) //
//				{
//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				}
//				bool isPageFull = 1 + (2*m + 4)*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
//				if (!isPageFull) // Leaf is not full
//				{
//					m++;
//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//					newEntry.pageId = -1; // Split does not happen
//				}
//
//
//				//				if (keyId < m) // 0 <= keyId <= m
//				//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				//				offset += 4;
//				//				memcpy((char *)curPageData + offset, &newEntry.pageId, sizeof(int));
//				//				bool isPageFull = 3*keyId
//				//				if (m < 2*d) // NL has space
//				//				{
//				//					//					cout << "NL[" << pageId << "] has space for the pushing/copying up key\n";
//				//					m++;
//				//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//				//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//				//					newEntry.pageId = -1; // Split does not happen
//				//				}
//				else // No space, push up key (without pageId)
//				{
//					// The first d entries [0, d-1]stay
//					int m1 = (m + 1)/2;
//					// Update m
//					memcpy((char *)curPageData + 1, &m1, sizeof(int));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest (m-d) entries [d+1, m] to new NL and push up [d]
//					void *newPageData = malloc(PAGE_SIZE);
//					isLeafNode = false;
//					int newNonLeafId;
//					int m_new = m - m1;
//					int newNonLeafId;
//					memcpy((char *)newPageData, &isLeafNode, 1);
//					memcpy((char *)newPageData + 1, &m_new, sizeof(int));
//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m1+3)*sizeof(int), (2*m_new+1)*sizeof(int)); // from p3
//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// The first d entries (key_1 to key_d) stay
//					//					m = d;
//					//					memcpy((char *)curPageData + 1, &m, sizeof(int));
//					//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest d entries (k_{d+2} to k_{2d+1}) to new NL, push up key_{d+1}
//					//					void *newPageData = malloc(PAGE_SIZE);
//					//					isLeafNode = false;
//					//					int newNonLeafId;
//					//					memcpy((char *)newPageData, &isLeafNode, 1); // false
//					//					memcpy((char *)newPageData + 1, &m, sizeof(int)); // m = d
//					//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m+3)*sizeof(int), (2*m+1)*sizeof(int)); // from p3
//					//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// Push up new entry to index
//					//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m1+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					newEntry.pageId = newNonLeafId; // ex: rChild = 7
//
//					// If root page splits, set new id to root
//					if (pageId == getRootId(ixfileHandle))
//					{
//						cout << "Split root, new root id[" << newNonLeafId << "], root_key = " << newEntry.key << endl;;
//						setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//						newEntry.pageId = -1; // End split
//					}
//					free(newPageData);
//					cout << "Split index page[" << pageId << "], push up (" << newEntry.key << ", " << newNonLeafId << ")\n";
//				}
//			}
//		}
//	}
//	else // Leaf
//	{
//		// flag, m, prev, next, k1, r1, k2, r2...
//		memcpy(&m, (char *)curPageData + 1, sizeof(unsigned)); // get number of valid data entries
//		int ptrFreeSpace;
//		int slotCount;
//		int entryOffset; // entry offset value in slot directory, not of the inserted entry!
//		memcpy(&ptrFreeSpace, (char *)curPageData + PAGE_SIZE - 2*sizeof(int), sizeof(int));
//		memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
//		// Remove all the invalid entryOffsets first
//		if (m < slotCount)
//		{
//			for (int i = 0; i < slotCount; i++)
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int));
//				if (entryOffset < 0)
//				{
//					if (i < slotCount - 1)
//					{
//						int slotDirOffset = PAGE_SIZE - (slotCount + 3)*sizeof(int);
//						memmove((char *)curPageData + slotDirOffset + 4, (char *)curPageData + slotDirOffset, (slotCount - i-1)*sizeof(int));
//					}
//					slotCount --;
//					if (m == slotCount)
//						break;
//				}
//			}
//			// Compress slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//		}
//		// If all elements in slot directory are -1, then slotCount = 0;
//		// Perform binary search for fixed-length <key, rid> entry
//		// Attention: returned keyId is the index of inserted valid entry
//		int keyId = searchKeyInLeaf(curPageData, m, key); // {70, 80, 90, || 100}, insert 95, returned key id = 3
//		int offset = 1 + (3*keyId+3)*sizeof(int);
//		//		cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << endl;
//		//				cout << "In leaf Write key = " << key << " at offset = " << offset << "\n";
//
//		// Insert this entry first, since the length of inserted entry is 12 bytes for int/float type
//		// If leaf is full after inserting, ignore the overwritten slot directory and split, create new slot dir for new leaf
//		memcpy((char *)curPageData + offset, &key, sizeof(int));
//		memcpy((char *)curPageData + offset + 4, &rid.pageNum, sizeof(int));
//		memcpy((char *)curPageData + offset + 4*2, &rid.slotNum, sizeof(int));
//		int insEntryLength = 3*sizeof(int); // Inserted entry length
//		bool isLastValidEntry = (keyId == m);
//		if (!isLastValidEntry) //
//		{
//			int movedEntriesLength = ptrFreeSpace - offset - insEntryLength;
//			memmove((char *)curPageData + offset + insEntryLength, (char *)curPageData + offset, movedEntriesLength);
//			// Update its following valid entries' offset
//			for (int i = slotCount; i >= keyId + 1; i--) // Read from array[slotCount - 1]
//			{
//				memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i + 3)*sizeof(int), sizeof(int));
//				entryOffset += insEntryLength;
//				memcpy((char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//		}
//		bool isPageFull = ptrFreeSpace + (slotCount+3)*sizeof(int) + 4*sizeof(int) > PAGE_SIZE; // EntryOffsets + Cnt + ptr + rootId
//		if (!isPageFull) // Leaf is not full
//		{
//			// Write inserted entryOffset
//			memcpy((char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int), &offset, sizeof(int));
//			slotCount ++; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			m++; // Update number of valid data entries (m)
//			memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//			// Update ptrFreeSpace
//			ptrFreeSpace += insEntryLength;
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//			newEntry.pageId = -1;
//		}
//		else // Leaf is full, split
//		{
//			// The first d entries [0, d-1]stay
//			int d = (m + 1)/2;
//			// Update m
//			memcpy((char *)curPageData + 1, &d, sizeof(int));
//			// Update ptrFreeSpace
//			ptrFreeSpace = 1 + (3*d+3)*sizeof(int);
//			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			slotCount = d; // Update slotCount
//			memcpy((char *)curPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//
//			//  Move the rest (m-d+1) entries [d, m] to new leaf and set it as child entry
//			void *newPageData = malloc(PAGE_SIZE);
//			isLeafNode = true;
//			int m_new = m - d + 1;
//			int prev = pageId; // New leaf's prev = curPageId
//			int next;
//			int newLeafId;
//			memcpy(&next, (char *)curPageData + 1 + 2*sizeof(int), sizeof(int)); // Cur leaf's next pageId
//			memcpy((char *)newPageData, &isLeafNode, 1);
//			memcpy((char *)newPageData + 1, &m_new, sizeof(int));
//			memcpy((char *)newPageData + 1 + sizeof(int), &prev, sizeof(int));
//			memcpy((char *)newPageData + 1 + 2*sizeof(int), &next, sizeof(int));
//			// Copy m_new data entries
//			memcpy((char *)newPageData + 1 + 3*sizeof(int), (char *)curPageData + 1 + (3*d+3)*sizeof(int), 3*m_new*sizeof(int));
//			// Update slotCount, ptrFreeSpace
//			slotCount = m_new;
//			memcpy((char *)newPageData + PAGE_SIZE - 3*sizeof(int), &slotCount, sizeof(int));
//			ptrFreeSpace = 1 + (3*m_new + 3)*sizeof(int);
//			memcpy((char *)newPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
//			// Create new slot directory
//			for (int i = 0; i < m_new; i++)
//			{
//				entryOffset = 1 + (3*i+3)*sizeof(int);
//				memcpy((char *)newPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), &entryOffset, sizeof(int));
//			}
//			ixfileHandle.fileHandle.appendPage(newPageData);
//
//			// Update curPageId's next pointer
//			newLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//			memcpy((char *)curPageData + 1 + 2*sizeof(int), &newLeafId, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//			// If curLeaf has next leaf, update prev pointer of L's next leaf
//			if (next >= 0)
//			{
//				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
//				memcpy((char *)curPageData + 1 + sizeof(int), &newLeafId, sizeof(int));
//				ixfileHandle.fileHandle.writePage(next, curPageData);
//			}
//
//			// Copy up new entry up to index
//			memcpy(&newEntry.key, (char *)newPageData + 1 + 3*sizeof(int), sizeof(int));
//			newEntry.pageId = newLeafId;
//
//			// If node resides in leaf page, create new root
//			if (pageId == getRootId(ixfileHandle))
//			{
//				setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//				cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << newEntry.key << endl;
//				newEntry.pageId = -1; // End split
//			}
//			free(newPageData);
//			//			cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newLeafId << "], ";
//			//			cout << "Copy up new entry (" << newEntry.key << ", " << newLeafId << ") to index!\n";
//		}
//	}
//	free(curPageData);
//}

//void IndexManager::insertKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageId, float &key, const RID &rid, FloatIndex &newEntry)
//{
//	int m;
//	bool isLeafNode;
//	void *curPageData = malloc(PAGE_SIZE);
//
//	ixfileHandle.fileHandle.readPage(pageId, curPageData);
//	memcpy(&isLeafNode, (char *)curPageData, 1);
//	memcpy(&m, (char *)curPageData + 1, sizeof(int));
//
//	if (!isLeafNode) // Non-leaf
//	{
//		int keyId = searchKeyInIndex(curPageData, m, key);
//		int childPageId;
//		memcpy(&childPageId, (char *)curPageData + 1 + (2*keyId+1)*sizeof(int), sizeof(int));
//		if (childPageId >= 0)
//		{
//			//			cout << "In index[" << childPageId << "]: \n";
//			insertKey(ixfileHandle, attribute, childPageId, key, rid, newEntry); // Recursive call
//			// If split happened in last recursion, add key index or push up it
//			if (newEntry.pageId >= 0)
//			{
//				// flag, m, p0, k1, p1, k2, p2, k3, p3... {5, 9, 13, 17, ||} + 19, keyId = 4, m = 4
//				keyId = searchKeyInIndex(curPageData, m, key); // <= key
//				int offset;
//				offset = 1 + (2*keyId+2)*sizeof(int);
//				if (keyId < m) // 0 <= keyId <= m
//					memmove((char *)curPageData + offset + 2*sizeof(int), (char *)curPageData + offset, (m - keyId)*2*sizeof(int));
//				memcpy((char *)curPageData + offset, &newEntry.key, sizeof(int));
//				offset += 4;
//				memcpy((char *)curPageData + offset, &newEntry.pageId, sizeof(int));
//				if (m < 2*d) // NL has space
//				{
//					//					cout << "NL[" << pageId << "] has space for the pushing/copying up key\n";
//					m++;
//					memcpy((char *)curPageData + 1, &m, sizeof(unsigned));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData); // pageId
//					newEntry.pageId = -1; // Split does not happen
//				}
//				else // No space, push up key (without pageId)
//				{
//					// The first d entries (key_1 to key_d) stay
//					m = d;
//					memcpy((char *)curPageData + 1, &m, sizeof(int));
//					ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//					//  Move the rest d entries (k_{d+2} to k_{2d+1}) to new NL, push up key_{d+1}
//					void *newPageData = malloc(PAGE_SIZE);
//					isLeafNode = false;
//					int newNonLeafId;
//					memcpy((char *)newPageData, &isLeafNode, 1); // false
//					memcpy((char *)newPageData + 1, &m, sizeof(int)); // m = d
//					memmove((char *)newPageData + 1 + sizeof(int), (char *)curPageData + 1 + (2*m+3)*sizeof(int), (2*m+1)*sizeof(int)); // from p3
//					ixfileHandle.fileHandle.appendPage(newPageData);
//
//					newNonLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//
//					// Push up new entry to index
//					memcpy(&newEntry.key, (char *)curPageData + 1 + (2*m+2)*sizeof(int), sizeof(int)); // from k3, ex: 13
//					newEntry.pageId = newNonLeafId; // ex: rChild = 7
//
//					// If root page splits, set new id to root
//					if (pageId == getRootId(ixfileHandle))
//					{
//						cout << "Split root, new root id[" << newNonLeafId << "], root_key = " << newEntry.key << endl;;
//						setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//						newEntry.pageId = -1; // End split
//					}
//					free(newPageData);
//					//					cout << "Split index page[" << pageId << "], push up (" << newEntry.key << ", " << newNonLeafId << ")\n";
//				}
//			}
//		}
//	}
//	else // Leaf
//	{
//		// flag, m, prev, next, k1, r1, k2, r2...
//		memcpy(&m, (char *)curPageData + 1, sizeof(unsigned)); // get number of data entries
//		// if (m == 0)?
//
//		int keyId = searchKeyInLeaf(curPageData, m, key); // {70, 80, 90, || 100}, insert 95, returned key id = 3
//		int offset = 1 + (3*keyId+3)*sizeof(int);
//		//		cout << "In leaf[" << pageId << "], m = " << m << ", keyId = " << keyId << endl;
//		//		cout << "In leaf Write key = " << key << " at offset = " << offset << "\n";
//		if (keyId + 1 < m) // keyId ranges [0, m]
//		{
//			//			cout << "Move\n";
//			memmove((char *)curPageData + offset + 12, (char *)curPageData + offset, (m - keyId)*3*sizeof(int));
//		}
//		memcpy((char *)curPageData + offset, &key, sizeof(int));
//		offset += 4;
//		memcpy((char *)curPageData + offset, &rid.pageNum, sizeof(int));
//		offset += 4;
//		memcpy((char *)curPageData + offset, &rid.slotNum, sizeof(int));
//		if (m < 2*d) // L has space
//		{
//			m++;
//			memcpy((char *)curPageData + 1, &m, sizeof(unsigned)); // Update m
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//			newEntry.pageId = -1;
//			//			cout << "Leaf has space, m = " << m << endl;
//		}
//		else // No space
//		{
//			// The first d entries stay
//			m = d;
//			memcpy((char *)curPageData + 1, &m, sizeof(int));
//			//  Move the rest (d+1) to new leaf and set it as child entry
//			void *newPageData = malloc(PAGE_SIZE);
//			isLeafNode = true;
//			m = d + 1;
//			int prev = pageId; // New leaf's prev = curPageId
//			int next;
//			int newLeafId;
//			memcpy(&next, (char *)curPageData + 1 + 2*sizeof(int), sizeof(int)); // Cur leaf's next pageId
//			memcpy((char *)newPageData, &isLeafNode, 1);
//			memcpy((char *)newPageData + 1, &m, sizeof(int));
//			memcpy((char *)newPageData + 1 + sizeof(int), &prev, sizeof(int));
//			memcpy((char *)newPageData + 1 + 2*sizeof(int), &next, sizeof(int));
//			// Copy m data entries
//			memcpy((char *)newPageData + 1 + 3*sizeof(int), (char *)curPageData + 1 + (3*m)*sizeof(int), 3*m*sizeof(int));
//			ixfileHandle.fileHandle.appendPage(newPageData);
//
//			// Update curPageId's next pointer
//			newLeafId = ixfileHandle.fileHandle.getNumberOfPages() - 1;
//			memcpy((char *)curPageData + 1 + 2*sizeof(int), &newLeafId, sizeof(int));
//			ixfileHandle.fileHandle.writePage(pageId, curPageData);
//
//			// If curLeaf has next leaf, update prev pointer of L's next leaf
//			if (next >= 0)
//			{
//				ixfileHandle.fileHandle.readPage(next, curPageData); // Read curLeaf's next page
//				memcpy((char *)curPageData + 1 + sizeof(int), &newLeafId, sizeof(int));
//				ixfileHandle.fileHandle.writePage(next, curPageData);
//			}
//
//			// Copy up new entry up to index
//			memcpy(&newEntry.key, (char *)newPageData + 1 + 3*sizeof(int), sizeof(int));
//			newEntry.pageId = newLeafId;
//
//			// If node resides in leaf page, create new root
//			if (pageId == getRootId(ixfileHandle))
//			{
//				setRootId(ixfileHandle, attribute, pageId, &newEntry.key, newEntry.pageId); // p0, 13, 7
//				cout << "Leaf: Create root(index), new root id[" << getRootId(ixfileHandle) << "], root_key = " << newEntry.key << endl;
//				newEntry.pageId = -1; // End split
//			}
//			free(newPageData);
//			//			cout << "Leaf[" << pageId << "] has no space, add new leaf[" << newLeafId << "], ";
//			//			cout << "Copy up new entry (" << newEntry.key << ", " << newLeafId << ") to index!\n";
//		}
//	}
//	free(curPageData);
//}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	if (!ixfileHandle.fileHandle.pFile)
		return -1; // File does not exist!
	RC rc;
	bool keyFound = false;
	void *rKey = malloc(100);
	RID returnedRId;
	IX_ScanIterator ix_ScanIterator;
	rc = scan(ixfileHandle, attribute, key, key, true, true, ix_ScanIterator);
	while(ix_ScanIterator.getNextEntry(returnedRId, rKey) == 0)
	{
		if (rid.pageNum == returnedRId.pageNum && rid.slotNum == returnedRId.slotNum)
		{
			//			cout << "In deleteEntry(): deleting entry key = " << *(float *)key << ", rid(" << rid.pageNum << ", " << rid.slotNum << ")\n";
			keyFound = true;
			// Lazy deletion
			int m;
			int ptrFreeSpace;
			int slotCount;
			int entryOffset;
			void *curPageData = malloc(PAGE_SIZE);
			ixfileHandle.fileHandle.readPage(ix_ScanIterator.leafPageNum, curPageData);

			memcpy(&m, (char *)curPageData + 1, 4);
			memcpy(&ptrFreeSpace, (char *)curPageData + PAGE_SIZE - 2*sizeof(int), sizeof(int));
			memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
			// Attention: after getNextEntry, keyId++
			int keyId = ix_ScanIterator.validKeyId - 1;
			memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - ((keyId+1) + 3)*sizeof(int), sizeof(int));

			// Delete key, rid, move forward if there is data behind
			//	 Restore the matching key id = ix_ScanIterator.keyId - 1, {0, 1, 2, 3}, m = 4

			int delEntryLength;
			if (attribute.type != TypeVarChar)
				delEntryLength = 3*sizeof(int);
			else
			{
				int varLength;
				memcpy(&varLength, (char *)curPageData + entryOffset, sizeof(int));
				delEntryLength = sizeof(int) + varLength;
			}
			int offsetIndex;
			//			cout << "Delete(): ";
			getOffsetIndex(curPageData, m, slotCount, keyId, offsetIndex, entryOffset);

			//			int offsetIndex;
			//			if (m < slotCount)
			//			{
			//				int validKeyCount = 0; 					// Find the keyId'th valid offset
			//				for (int i = 0; i < slotCount; i++)
			//				{
			//					memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), sizeof(int));
			//					if (entryOffset > 0)
			//					{
			//						if (validKeyCount == keyId)
			//						{
			//							offsetIndex = i;
			//							break;
			//						}
			//						validKeyCount ++;
			//					}
			//				}
			//			}
			//			else
			//				offsetIndex = keyId;
			//			memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (offsetIndex+1 + 3)*sizeof(int), sizeof(int));
			int movedEntriesLength = ptrFreeSpace - entryOffset - delEntryLength;
			bool isLastValidEntry = (movedEntriesLength == 0);
			//			cout << "mvLen = " << movedEntriesLength << ", eO = " << entryOffset << ", oI = " << offsetIndex << endl;
			if (!isLastValidEntry) // Not the last valid key, move required!
			{
				//				cout << "DeleteEntry(): keyId = " << keyId << "\n";
				memmove((char *)curPageData + entryOffset, (char *)curPageData + entryOffset + delEntryLength, movedEntriesLength);
				// Update entryOffset of its following valid entry (curEntryOffset > 0)
				for (int j = offsetIndex + 1; j < slotCount; j++)
				{
					memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - ((j+1) + 3)*sizeof(int), sizeof(int));
					if (entryOffset > 0)
					{
						//												cout << "update offset[" << j << "], ";
						entryOffset -= delEntryLength;
						//												cout << entryOffset << endl;
						memcpy((char *)curPageData + PAGE_SIZE - ((j+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
					}
				}
			}
			// Set deleted entry's entryOffet = -1
			entryOffset = -1;
			memcpy((char *)curPageData + PAGE_SIZE - ((offsetIndex+1) + 3)*sizeof(int), &entryOffset, sizeof(int));
			// Update ptrFreeSpace
			ptrFreeSpace -= delEntryLength;
			memcpy((char *)curPageData + PAGE_SIZE - 2*sizeof(int), &ptrFreeSpace, sizeof(int));
			// Update number of valid data entries (m)
			m--;
			memcpy((char *)curPageData + 1, &m, 4);
			//			cout << "DeleteEntry(): m goes down to " << m << "\n";
			ixfileHandle.fileHandle.writePage(ix_ScanIterator.leafPageNum, curPageData);
			free(curPageData);
		}
		//		else
		//		{
		////			cout << "DeleteE(): returned key = " << *(int *)rKey << ", rid(" << returnedRId.pageNum << ", " << returnedRId.slotNum << ")\n";
		//		}
	}

	free(rKey);
	ix_ScanIterator.close();
	if (!keyFound)
	{
		cout << "Deleted entry key = " << *(int *)key << ", rid(" << rid.pageNum << ", " << rid.slotNum << ") does not exist!\n";
		return -1; // return error when deleted entry does not exist
	}
	//	cout << "delete end\n";
	return 0;
}

void getOffsetIndex(void *curPageData, int m, int slotCount, int keyId, int &offsetIndex, int &entryOffset)
{
	//	memcpy(&m, (char *)curPageData + 1, sizeof(int));
	//	memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));

	if (m < slotCount)
	{
		int validKeyCount = 0; 					// Find the keyId'th valid offset
		for (int i = 0; i < slotCount; i++)
		{
			int newEntryOffset;
			memcpy(&newEntryOffset, (char *)curPageData + PAGE_SIZE - ((i+1) + 3)*sizeof(int), sizeof(int));
			if (newEntryOffset > 0)
			{
				if (validKeyCount == keyId)
				{
					offsetIndex = i;
					break;
				}
				validKeyCount ++;
			}
		}
	}
	else
		offsetIndex = keyId;
	memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (offsetIndex+1 + 3)*sizeof(int), sizeof(int));

	//	cout << "m = " << m << ", sC = " << slotCount << ", k = " << keyId << "\n";
	//	return offsetIndex;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
		const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator) {
	// lowkey NULL <highkey, highkey low NULL>lowkey
	bool fileTreeExist = ixfileHandle.fileHandle.pFile && ixfileHandle.fileHandle.getNumberOfPages() > 0;
	if (!fileTreeExist) // Tree is empty, scan failed!
		return -1;
	ix_ScanIterator.set(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
	return 0;
}

// Read root id from disk file
int getRootId(IXFileHandle &ixfileHandle)
{
	bool fileTreeExist = ixfileHandle.fileHandle.pFile && ixfileHandle.fileHandle.getNumberOfPages() > 0;
	if (!fileTreeExist) // File does not exist, get root id failed!
		return -1;
	int rootId;
	void *curPageData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, curPageData);
	memcpy(&rootId, (char *)curPageData + PAGE_SIZE - sizeof(int), sizeof(int));
	free(curPageData);
	return rootId;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute)
{
	int rootId = getRootId(ixfileHandle);
	bool isPrevKeyExist = false;
	if (rootId < 0)
		cout << "File does not exist or B+ Tree is empty!\n";
	else // Depth first, pre-order
		printNode(ixfileHandle, attribute, rootId, isPrevKeyExist);
	cout << endl;
}

// Used in printBTree
void IndexManager::printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int nodeId, bool &isPrevKeyExist)
{
	void *curPageData = malloc(PAGE_SIZE);
	RID rid;
	bool isSameKey = false;

	ixfileHandle.fileHandle.readPage(nodeId, curPageData); // Read a page
	int m = *(int *)((char *)curPageData + 1);
	bool isLeafNode = *(bool *)((char *)curPageData);
	bool isRootPage = (nodeId == getRootId(ixfileHandle));
	cout << "{";
	if (isRootPage && !isLeafNode)
		cout << "\n";
	cout << "\"keys\": ["; // Start of keys
	if (isLeafNode) // Leaf page, print all the entries
	{
		if (attribute.type == TypeInt)
		{
			int prevKey;
			for (int i = 0; i < m; i++)
			{
				int key = *(int *)((char *)curPageData + 1 + (3*i+3)*sizeof(int));
				rid.pageNum = *(int *)((char *)curPageData + 1 + (3*i+4)*sizeof(int));
				rid.slotNum = *(int *)((char *)curPageData + 1 + (3*i+5)*sizeof(int));
				if (!isPrevKeyExist) // Initialize prevKey
				{
					prevKey = key;
					isPrevKeyExist = true;
					isSameKey = false; // print a new key
				}
				else
				{
					if (prevKey == key)
						isSameKey = true; // Do not print a duplicate key
					else
					{
						prevKey = key;
						isSameKey = false;
					}
				}
				if (isSameKey) // DO not print key
					cout << ", (" << rid.pageNum << ", "<< rid.slotNum << ")";
				else // Print new key
				{
					if (isPrevKeyExist && i > 0)
						cout << "]\", ";
					cout << "\"" ;
					cout << key <<  ": [(" << rid.pageNum << ", "<< rid.slotNum << ")";
				}
			}
		}
		else if (attribute.type == TypeReal)
		{
			float prevKey;
			for (int i = 0; i < m; i++)
			{
				float key = *(int *)((char *)curPageData + 1 + (3*i+3)*sizeof(int));
				rid.pageNum = *(int *)((char *)curPageData + 1 + (3*i+4)*sizeof(int));
				rid.slotNum = *(int *)((char *)curPageData + 1 + (3*i+5)*sizeof(int));
				if (!isPrevKeyExist) // Initialize prevKey
				{
					prevKey = key;
					isPrevKeyExist = true;
					isSameKey = false; // print a new key
				}
				else
				{
					if (prevKey == key)
						isSameKey = true; // Do not print a duplicate key
					else
					{
						prevKey = key;
						isSameKey = false;
					}
				}
				if (isSameKey) // DO not print key
					cout << ", (" << rid.pageNum << ", "<< rid.slotNum << ")";
				else // Print new key
				{
					if (isPrevKeyExist && i > 0)
						cout << "]\", ";
					cout << "\"" ;
					cout << key <<  ": [(" << rid.pageNum << ", "<< rid.slotNum << ")";
				}
			}
		}
		else // TypeVarChar
		{
			vector<int> entryOffsets;
			RC rc = getValidEntryOffsets(curPageData, entryOffsets);
			assert(rc == 0 && "getValidEntryOffsets() failed!");
			int prevLen;
//			char *prevKey = (char *)malloc(attribute.length);
			char *prevKey;
			for (int i = 0; i < m; i++)
			{
				int entryOffset = entryOffsets[i];
				int keyLength = *(int *)((char *)curPageData + entryOffset);
				entryOffset += 4;
				char *key = (char *)malloc(keyLength);
				memcpy(key, (char *)curPageData + entryOffset, keyLength);
				if (!isPrevKeyExist) // Initialize prevKey
				{
					prevLen = keyLength;
					prevKey = (char *)malloc(prevLen);
					memcpy(prevKey, key, keyLength);
					isPrevKeyExist = true;
					isSameKey = false; // print a new key
				}
				else
				{
					int k_pK = memcmp(key, prevKey, prevLen);
					if (prevKey[0] == 'o' && isPrintNode)
						cout << "\nk = " << key << keyLength << ", pK = " << prevKey << prevLen
						<< "memcmp = " << k_pK << endl;
					if (prevLen == keyLength && k_pK == 0)
					{
							isSameKey = true; // Do not print a duplicate key
					}
					else
					{
						free(prevKey);
						prevLen = keyLength;
						prevKey = (char *)malloc(prevLen);
						memcpy(prevKey, key, keyLength); // update prevKey
						isSameKey = false;
					}
				}
				entryOffset += keyLength;
				memcpy(&rid.pageNum, (char *)curPageData + entryOffset, sizeof(int));
				entryOffset += 4;
				memcpy(&rid.slotNum, (char *)curPageData + entryOffset, sizeof(int));
				if (isSameKey) // DO not print key
					cout << ", (" << rid.pageNum << ", "<< rid.slotNum << ")";
				else // Print new key
				{
					if (isPrevKeyExist && i > 0)
						cout << "]\", ";
					cout << "\"" ;
//					cout << *key << keyLength;
					if (keyLength < 100)
					{
						for (int j = 0; j < keyLength; j++)
							cout << *(key + j);
					}
					else
						cout << *key << "(" << keyLength << ")";
					cout <<  ": [(" << rid.pageNum << ", "<< rid.slotNum << ")";
				}
				free(key);
			}
			free(prevKey);
			cout << "]\""; // end mark of last key
			entryOffsets.clear();
		}
		cout << "]"; // End of keys
	}
	else // Index page: Continue to scan children, p_0...p_m
	{
		if (attribute.type == TypeInt)
		{   // Print m keys in the node
			for (int i = 0; i < m; i++) // key_2 to key_m
			{
				int key = *(int *)((char *)curPageData + 1 + (2*i+2)*sizeof(int));
				if (i > 0)
					cout << ", ";
				cout << "\"" << key << "\"";
			}
			// Print m+1 children
			cout << "],\n\"children\": [\n"; // End of keys and start of children
			for (int i = 0; i <= m; i++) // child_1 to child_m
			{
				if (i > 0)
					cout << ",\n"; // Separator of children of more than 1
				int pageId = *(int *)((char *)curPageData + 1 + (2*i+1)*sizeof(int));
				if (pageId != -1)
					printNode(ixfileHandle, attribute, pageId, isPrevKeyExist);
				else
					cout << "{\"keys\": [\"\"]}"; // Empty child
			}
		}
		else if (attribute.type == TypeReal)
		{   // Print m keys in the node
			for (int i = 0; i < m; i++)
			{
				float key = *(int *)((char *)curPageData + 1 + (2*i+2)*sizeof(int));
				if (i > 0)
					cout << ", ";
				cout << "\"" << key << "\"";
			}
			// Print m+1 children
			cout << "],\n\"children\": [\n"; // End of keys and start of children
			for (int i = 0; i <= m; i++) // child_1 to child_m
			{
				if (i > 0)
					cout << ",\n"; // Separator of children
				int pageId = *(int *)((char *)curPageData + 1 + (2*i+1)*sizeof(int));
				if (pageId != -1)
					printNode(ixfileHandle, attribute, pageId, isPrevKeyExist);
				else
					cout << "{\"keys\": [\"\"]}"; // Empty child
			}
		}
		else // TypeVarChar
		{   // Print m keys in the node
			for (int i = 0; i < m; i++)
			{
				int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+1+3)*4);
				int keyLength = *(int *)((char *)curPageData + entryOffset);
				char *key = (char *)malloc(keyLength);
				memcpy(key, (char *)curPageData + entryOffset + 4, keyLength);
				if (i > 0)
					printf(", ");
				cout <<	"\"" ;
				if (keyLength < 100)
				{
					for (int j = 0; j < keyLength; j++)
						cout << *(key+j);
				}
				else
					cout << *key << "(" << keyLength << ")";
				cout << "\"";
				free(key);
			}
			// Print m+1 children
			cout << "],\n\"children\": [\n"; // End of keys and start of children
			for (int i = 0; i <= m; i++) // child_1 to child_m
			{
				if (i > 0)
					cout << ",\n"; // Separator of children
				int pageId;
				if (i == 0)
					pageId = *(int *)((char *)curPageData + 5);
				else
				{
					int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (i+3)*sizeof(int));
					assert(entryOffset < PAGE_SIZE && "Ptree() failed");
					int len = *(int *)((char *)curPageData + entryOffset);
					pageId = *(int *)((char *)curPageData + entryOffset + 4 + len);
				}
				if (pageId != -1)
				{
					isPrevKeyExist = false;
					printNode(ixfileHandle, attribute, pageId, isPrevKeyExist);
					assert(pageId != getRootId(ixfileHandle) && "PrintTree() failed");
					if (i == m)
						cout << "\n";
				}
				else
					cout << "{\"keys\": [\"\"]}"; // Empty child
			}
		}
		cout << "]"; // End of children
	}
	free(curPageData);
	if (isRootPage && !isLeafNode)
		cout << "\n";
	cout << "}"; // End of keys
//	if (!isLeafNode)
//		cout << "\n";
}

IX_ScanIterator::IX_ScanIterator() {
	// Initialize bounds
	this->leafPageStart = -1;
	this->leafPageEnd = -1;
	this->keyStart = -1;
	this->keyEnd = -1;
	// Keys
	this->lowKey = NULL;
	this->highKey = NULL;
	this->lowKeyInclusive = false;
	this->highKeyInclusive = false;
}

IX_ScanIterator::~IX_ScanIterator() {
	if (lowKey)
		free(lowKey);
	if (highKey)
		free(highKey);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if (leafPageStart < 0 || keyStart < 0 || leafPageEnd < 0 || keyEnd < 0) // No matching keys returned, exit!
		return -1;
	void *curPageData = malloc(PAGE_SIZE);
	int nextPageNum; // Next leaf page number
	int m; // Number of valid keys in a data entry
	int entryOffset;
	int slotCount;

	ixfileHandle.fileHandle.readPage(leafPageNum, curPageData);
	memcpy(&nextPageNum, (char *)curPageData + 1 + 2*sizeof(int), sizeof(unsigned)); // Get next leaf
	memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
	// Get number of matching entries in leaf
	if (leafPageNum != leafPageEnd)
		memcpy(&m, (char *)curPageData + 1, sizeof(int));
	else // Reaching the last matching page, the number of matching entries is keyEnd
		m = keyEnd + 1;
	// Attention: the value of m in leafPageStart and leafPageEnd will not be 0, thanks to getLowerBound/getUpperBound functions

	// Exceptions: within the selected leaf range, some leaf is empty, m = 0
	bool readNextLeaf = (validKeyId + 1 > m);
	while (readNextLeaf)
	{
		if (leafPageNum == leafPageEnd) // Reaching the last page, exit!
		{
			free(curPageData);
			return -1;
		}
		validKeyId = 0;
		leafPageNum = nextPageNum;
		ixfileHandle.fileHandle.readPage(leafPageNum, curPageData);
		memcpy(&nextPageNum, (char *)curPageData + 1 + 2*sizeof(int), sizeof(unsigned)); // Get next leaf
		memcpy(&slotCount, (char *)curPageData + PAGE_SIZE - 3*sizeof(int), sizeof(int));
		if (nextPageNum != leafPageEnd)
			memcpy(&m, (char *)curPageData + 1, sizeof(unsigned)); // Get number of entries
		else
			m = keyEnd + 1;
		readNextLeaf = (validKeyId + 1 > m);
		//		cout << "Read leaf[" << leafPageNum << "], key[" << keyId << "], m = " << m << "\n";
	}


	// Read a key and a rid, get entry offset, in case of deleteEntry() inside scan-getNextEntry-while loop
	//	int offsetIndex;
	//	cout << "getNextEntry(): ";
	//	getOffsetIndex(curPageData, m, slotCount, validKeyId, offsetIndex, entryOffset);
	if (m < slotCount) // There are some deleted slots in slot dir, skip them
	{
		int validCount = 0;
		for(int i = 0; i < slotCount; i++)
		{
			memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (i+1 + 3)*sizeof(int), sizeof(int));
			if (entryOffset > 0)
			{
				if (validCount == validKeyId)
					break;
				validCount ++;
			}
		}
	}
	else
		memcpy(&entryOffset, (char *)curPageData + PAGE_SIZE - (validKeyId+1 + 3)*sizeof(int), sizeof(int));
	if (attribute.type != TypeVarChar) // Leaf page format: flag, m, prev, next, k1, rid1...
	{
		memcpy(key, (char *)curPageData + entryOffset, sizeof(int));
		memcpy(&rid.pageNum, (char *)curPageData + entryOffset + 4, sizeof(unsigned));
		memcpy(&rid.slotNum, (char *)curPageData + entryOffset + 4*2, sizeof(unsigned));
		//		if (rid.pageNum % 500 == 0)
		//			cout << "key = " <<  *(int *)key <<  ", returned rid(" << rid.pageNum << ")\n";
	}
	else // TypeVarChar
	{
		int varLength = *(int *)((char *)curPageData + entryOffset);
		memcpy((char *)key, (char *)curPageData + entryOffset, 4 + varLength);
		memcpy(&rid.pageNum, (char *)curPageData + entryOffset + 4 + varLength, sizeof(unsigned));
		memcpy(&rid.slotNum, (char *)curPageData + entryOffset + 4*2 + varLength, sizeof(unsigned));
		if (varLength == 20)
			cout << *((char *)key+4) << ", ";
	}
	validKeyId++;
	//	cout << "KeyId = " << keyId << ", v_k = " << validKeyId << endl;
	free(curPageData);
	return 0;
}

RC IX_ScanIterator::close() {
	// Clear work
	this->leafPageStart = -1;
	this->leafPageEnd = -1;
	this->keyStart = -1;
	this->keyEnd = -1;
	// Keys
	this->lowKey = NULL;
	this->highKey = NULL;
	this->lowKeyInclusive = false;
	this->highKeyInclusive = false;
	return 0;
}

// Set member values of ix_ScanIterator
void IX_ScanIterator::set(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
		const void *highKey, bool lowKeyInclusive, bool highKeyInclusive)
{
	this->ixfileHandle = ixfileHandle;
	this->attribute = attribute;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;
	bool keyValid = true;
	if (lowKey && highKey) // lowKey should <= highKey
	{
		if (attribute.type == TypeInt)
		{
			if (*(int *)lowKey > *(int *)highKey)
				keyValid = false;
		}
		else if (attribute.type == TypeReal)
		{
			if (*(float *)lowKey > *(float *)highKey)
				keyValid = false;
		}
		else
		{
			if (strcmp_1((char *)lowKey, (char *)highKey) > 0)
				keyValid = false;
		}
	}
	if (keyValid)
	{
		//		cout << "low & high key is valid\n";
		if (lowKey) // Copy content of lowKey and highKey if not NULL
		{
			if (attribute.type == TypeInt)
			{
				this->lowKey = (int *)malloc(sizeof(int));
				*((int *)this->lowKey) =  *((int *)lowKey);
			}
			else if (attribute.type == TypeReal)
			{
				this->lowKey = (float *)malloc(sizeof(float));
				*((float *)this->lowKey) =  *((float *)lowKey);
			}
			else
			{
				int len = *(int *)lowKey;
				this->lowKey = (char *)malloc(len + 4);
				memcpy((char *)this->lowKey, (char *)lowKey, len + 4);
			}
		}
		if (highKey)
		{
			if (attribute.type == TypeInt)
			{
				this->highKey = (int *)malloc(sizeof(int));
				*((int *)this->highKey) =  *((int *)highKey);
			}
			else if (attribute.type == TypeReal)
			{
				this->highKey = (float *)malloc(sizeof(float));
				*((float *)this->highKey) =  *((float *)highKey);
			}
			else
			{
				int len = *(int *)highKey;
				this->highKey = (char *)malloc(len + sizeof(int));
				memcpy((char *)this->highKey, (char *)highKey, len + sizeof(int));
//				cout << "Scan (" << *((char *)lowKey+4) << len << ", " << *((char *)highKey+4) << len << ")..\n";
			}
		}
		int rootId = getRootId(ixfileHandle);
		RC rc = getLowerBound(rootId, attribute);
//		printDataEntry(ixfileHandle, leafPageStart, keyStart, attribute);
		if (rc == 0 && leafPageStart >= 0 && keyStart >= 0)
			getUpperBound(rootId, attribute);
		// Initialize leafPageNum, keyId
//		printDataEntry(ixfileHandle, leafPageEnd, keyEnd, attribute);
		leafPageNum = leafPageStart;
		validKeyId = keyStart;
	}
	else
		cout << "Scan failed, lowKey should <= highKey!\n";
	if (isShowScan)
		cout << "Matched keys: from leaf[" << leafPageStart << "] , key[" << keyStart << "] to leaf[" << leafPageEnd << "], key[" << keyEnd << "]\n";
}

// Get the 1st of matching leaf and key id
RC IX_ScanIterator::getLowerBound(int pageId, const Attribute &attribute)
{
	bool isLeafNode;
	int m; // Number of entries
	void *curPageData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, curPageData);
	memcpy(&isLeafNode, (char *)curPageData, 1);

	int childPageId = pageId;
	int keyId; // Id of Key in a page
	bool isKeyInIndex;

	while(!isLeafNode) // Find the leaf where the desired key belongs
	{
		m = *(int *)((char *)curPageData + 1);
		if (!lowKey) // Find key > -oo, return 1st key in leaf
		{
			keyId = 0; // Proceed with its 1st non-NULL child[i]
			childPageId = *(int *)((char *)curPageData + 5);
		}
		else // Choose subtree
		{
			if (attribute.type == TypeInt)
			{
				keyId = this->searchKeyInIndex_scan(curPageData, m, *(int *)lowKey, false, lowKeyInclusive, isKeyInIndex);
				childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
			}
			else if (attribute.type == TypeReal)
			{
				keyId = this->searchKeyInIndex_scan(curPageData, m, *(float *)lowKey, false, lowKeyInclusive, isKeyInIndex);
				childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
			}
			else
			{
				keyId = this->searchKeyInIndex_scan(curPageData, m, (char *)lowKey, false, lowKeyInclusive, isKeyInIndex);
				int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId +3)*sizeof(int));
				assert(entryOffset < PAGE_SIZE && "getLowerBound() failed, invalid entryOffset!");
				if (isGetBound)
					cout << "keyId = " << keyId << endl;
				if (keyId)
				{
					int varLen = *(int *)((char *)curPageData + entryOffset);
					childPageId = *(int *)((char *)curPageData + entryOffset + 4 + varLen);
				}
				else // keyId == 0
					childPageId = *(int *)((char *)curPageData + 5);
				assert(childPageId < ixfileHandle.fileHandle.getNumberOfPages() && "getLowerBound() failed, invalid childPageId!");
			}
		}
		ixfileHandle.fileHandle.readPage(childPageId, curPageData); // Go down a level if possible
		if (isGetBound)
			cout << "GetLowerBound(): Go down to page[" << childPageId << "]...\n";
		memcpy(&isLeafNode, (char *)curPageData, 1);
	}

	// If the returned leaf is empty, find next non-empty leaf
	m = *(int *)((char *)curPageData + 1);
	while (m == 0)
	{
		int offset = 1 + 2*sizeof(int);
		int nextLeafId = *(int *)((char *)curPageData + offset);
		if (nextLeafId >= 0)
		{
			ixfileHandle.fileHandle.readPage(nextLeafId, curPageData);
			m = *(int *)((char *)curPageData + 1);
		}
		else // = -1, reaches end page of leaf level
			return -1; // The last page also does not have any keys, getLowerBound() failed!
	}

	// Here the leaf must have at least one key. Search desired key in leaf
	this->leafPageStart = childPageId;
	if (!lowKey)
		this->keyStart = 0;
	else
	{
		if (attribute.type == TypeInt)
			this->keyStart = this->searchKeyInLeaf_scan(curPageData, m, *(int *)lowKey, false, lowKeyInclusive, isKeyInIndex);
		else if (attribute.type == TypeReal)
			this->keyStart = this->searchKeyInLeaf_scan(curPageData, m, *(float *)lowKey, false, lowKeyInclusive, isKeyInIndex);
		else
			this->keyStart = this->searchKeyInLeaf_scan(curPageData, m, (char *)lowKey, false, lowKeyInclusive, isKeyInIndex);
	}
	if (isGetBound)
		cout << "getLowerBound() at leaf[" << leafPageStart << "], key[" << keyStart << "]\n";
	free(curPageData);
	return 0;
}

// Get the last matching leaf and key id
RC IX_ScanIterator::getUpperBound(int pageId, const Attribute &attribute) // < highKey or <= highKey
{
	bool isLeafNode;
	int m; // Number of entries
	void *curPageData = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageId, curPageData);
	memcpy(&isLeafNode, (char *)curPageData, 1);

	int childPageId = pageId;
	int keyId; // Id of Key in a page
	bool isKeyInIndex;

	while(!isLeafNode) // Find the leaf where the desired key belongs
	{
		m = *(int *)((char *)curPageData + 1);
		if (!highKey) // Find key > -oo, return 1st key in leaf
		{
			keyId = m; // Proceed with its 1st non-NULL child[i]
			if (attribute.type != TypeVarChar)
				childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
			else
			{
				vector<int> entryOffsets;
				getValidEntryOffsets(curPageData, entryOffsets);
				int len = *(int *)((char *)curPageData + entryOffsets[m-1]);
				childPageId = *(int *)((char *)curPageData + entryOffsets[m-1] + 4 + len);
			}
			if (isGetBound)
				cout << "GetUppderBound(): childPageId = " << childPageId << ", m = " << m << "\n";
		}
		else // Choose subtree
		{
			if (attribute.type == TypeInt)
			{
				keyId = searchKeyInIndex_scan(curPageData, m, *(int *)highKey, true, highKeyInclusive, isKeyInIndex);
				childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
			}
			else if (attribute.type == TypeReal)
			{
				keyId = searchKeyInIndex_scan(curPageData, m, *(float *)highKey, true, highKeyInclusive, isKeyInIndex);
				childPageId = *(int *)((char *)curPageData + 1 + (2*keyId+1)*sizeof(int));
			}
			else
			{
				keyId = this->searchKeyInIndex_scan(curPageData, m, (char *)highKey, true, highKeyInclusive, isKeyInIndex);
				int entryOffset = *(int *)((char *)curPageData + PAGE_SIZE - (keyId +3)*sizeof(int));
				assert(entryOffset < PAGE_SIZE && "getUpperBound() failed, invalid entryOffset!");
				if (keyId)
				{
					int varLen = *(int *)((char *)curPageData + entryOffset);
					childPageId = *(int *)((char *)curPageData + entryOffset + 4 + varLen);
				}
				else
					childPageId = *(int *)((char *)curPageData + 5);
				assert(childPageId < ixfileHandle.fileHandle.getNumberOfPages() && "getLowerBound() failed, invalid childPageId!");
			}
		}
		ixfileHandle.fileHandle.readPage(childPageId, curPageData); // Go down a level if possible
		if (isGetBound)
			cout << "GetUpperBound(): Go down to page[" << childPageId << "]...\n";
		memcpy(&isLeafNode, (char *)curPageData, 1);
	}

	// If the returned leaf is empty, find previous non-empty leaf
	m = *(int *)((char *)curPageData + 1);
	while (m == 0)
	{
		int offset = 1 + sizeof(int);
		int prevLeafId = *(int *)((char *)curPageData + offset);
		if (prevLeafId >= 0)
		{
			ixfileHandle.fileHandle.readPage(prevLeafId, curPageData);
			m = *(int *)((char *)curPageData + 1);
		}
		else // = -1, reaches start page of leaf level
			return -1; // The first page also does not have any keys, getUpperBound() failed!
	}

	// Here the leaf must have at least one key. Search desired key in leaf
	this->leafPageEnd = childPageId;
	if (!highKey)
		this->keyEnd = m - 1;
	else
	{
		if (attribute.type == TypeInt)
			this->keyEnd = this->searchKeyInLeaf_scan(curPageData, m, *(int *)highKey, true, highKeyInclusive, isKeyInIndex);
		else if (attribute.type == TypeReal)
			this->keyEnd = this->searchKeyInLeaf_scan(curPageData, m, *(float *)highKey, true, highKeyInclusive, isKeyInIndex);
		else
			this->keyEnd = this->searchKeyInLeaf_scan(curPageData, m, (char *)highKey, true, highKeyInclusive, isKeyInIndex);
	}
	if (isGetBound)
		cout << "getUpperBound() at leaf[" << leafPageEnd << "], key[" << keyEnd << "]\n";
	free(curPageData);
	return 0;
}

// INT: searchKeyInIndex returns the index of child pointer, ranging from [0, m]
int IX_ScanIterator::searchKeyInIndex_scan(void *data, int numIndex, int key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
	int leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numIndex - 1; // numIndex >= 1
	int offset = 1 + 2*sizeof(int);
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (2*rightIndex)*sizeof(int), sizeof(int));
	if (key >= rightKey)		// Go down to child[m]
		return numIndex;
	else if (key < leftKey)		// Go down to child[0]
		return 0;
	else // key[0] <= key < key[m]
	{
		int midIndex;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + (2*midIndex)*sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else if (key > midKey)
				leftIndex = midIndex;
			else // key == midKey, ex: {270, 280, 290, 300}, key = 290
			{
				isKeyInIndex  = true;
				// Exceptions: key < 290, go to [280, 290), return 2; otherwise, go to [290, 300), return 3
				if (isHighKey && !Inclusive)
					return midIndex;
				else
					return midIndex + 1;
			}
			//		cout << "Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
		}
		return rightIndex; // Key does not appear in indexes, key = 285, return 2
	}
}


// REAL: searchKeyInIndex returns the index of child pointer, ranging from [0, m]
int IX_ScanIterator::searchKeyInIndex_scan(void *data, int numIndex, float key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
	float leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numIndex - 1; // numIndex >= 1
	int offset = 1 + 2*sizeof(int);
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (2*rightIndex)*sizeof(int), sizeof(int));
	if (key >= rightKey)		// Go down to child[m]
		return numIndex;
	else if (key < leftKey)		// Go down to child[0]
		return 0;
	else // key[0] <= key < key[m]
	{
		int midIndex;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + (2*midIndex)*sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else if (key > midKey)
				leftIndex = midIndex;
			else // key == midKey, ex: {270, 280, 290, 300}, key = 290
			{
				isKeyInIndex  = true;
				// Exceptions: key < 290, go to [280, 290), return 2; otherwise, go to [290, 300), return 3
				if (isHighKey && !Inclusive)
					return midIndex;
				else
					return midIndex + 1;
			}
		}
		return rightIndex; // Key does not appear in indexes, key = 285, return 2
	}
}

void printIndexPage(void *data)
{
	int m = *(int *)((char *)data + 1);
	vector<int> entryOffsets;
	getValidEntryOffsets(data, entryOffsets);
	RID rid;
	for(int i = 0; i < m; i++)
	{
		int len = *(int *)((char *)data + entryOffsets[i]);
		char *key = (char *)malloc(len);
		memcpy(key, (char *)data + entryOffsets[i] + 4, len);
		rid.pageNum = *(int *)((char *)data + entryOffsets[i] + 4 + len);
		cout << *key << len << ": " << rid.pageNum << ", ";
		free(key);
	}
}

// VARCHAR
int IX_ScanIterator::searchKeyInIndex_scan(void *data, int numIndex, char* key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
//	printIndexPage(data);
	int leftIndex = 0;
	int rightIndex = numIndex - 1;
	int entryOffset = *(int *)((char *)data + PAGE_SIZE - (leftIndex+1 + 3)*sizeof(int));
	int varLength = *(int *)((char *)data + entryOffset);
	assert(varLength <= attribute.length && "IX_ScanIterator::searchKeyInIndex_scan(): invalid varLength!");
	char *leftKey = (char *)malloc(varLength + 4);
	memcpy(leftKey, (char *)data + entryOffset, 4 + varLength);

	entryOffset = *(int *)((char *)data + PAGE_SIZE - (rightIndex+1 + 3)*sizeof(int));
	varLength = *(int *)((char *)data + entryOffset);
	char *rightKey = (char *)malloc(varLength + 4);
	memcpy(rightKey, (char *)data + entryOffset, 4 + varLength);
	if (isScanIndex)
		cout << "srch " << *(key+4) << "...";

	if (strcmp_1(key, rightKey) >= 0)
	{
		free(leftKey);
		free(rightKey);
		return numIndex;
	}
	else if (strcmp_1(key, leftKey) < 0)
	{
		free(leftKey);
		free(rightKey);
		return 0;
	}
	else
	{
		free(leftKey);
		free(rightKey);
		while (leftIndex + 1 < rightIndex)
		{
			int midIndex = (leftIndex + rightIndex) / 2;
			entryOffset = *(int *)((char *)data + PAGE_SIZE - (midIndex+1 + 3)*sizeof(int));
			varLength = *(int *)((char *)data + entryOffset);
			char *midKey = (char *)malloc(varLength + 4);
			memcpy(midKey, (char *)data + entryOffset, 4 + varLength);
			int key_midKey = strcmp_1(key, midKey);
			if (key_midKey < 0)
				rightIndex = midIndex;
			else if (key_midKey > 0)
				leftIndex = midIndex;
			else // key == midKey, ex: {270, 280, 290, 300}, key = 290
			{
				isKeyInIndex  = true;
				free(midKey);
				// Exceptions: key < 290, go to [280, 290), return 2; otherwise, go to [290, 300), return 3
				if (isHighKey && !Inclusive)
					return midIndex;
				else
					return midIndex + 1;
			}
			if (isScanIndex)
				cout << "ScanKeyInIndex: l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << *(char *)midKey << endl;
			free(midKey);
		}
		return rightIndex;
	}
}


template<class T>
int searchKeyInIndex(void *data, int numIndex, T key)
{
	T leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numIndex - 1; // numIndex >= 1
	int offset = 1 + 2*sizeof(int);
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (2*rightIndex)*sizeof(int), sizeof(int));
	if (key >= rightKey)		// Go down to child[m]
		return numIndex;
	else if (key < leftKey)		// Go down to child[0]
		return 0;
	else // key[0] <= key < key[m]
	{
		int midIndex;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + (2*midIndex)*sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else if (key > midKey)
				leftIndex = midIndex;
			else // key == midKey, ex: {270, 280, 290, 300}, key = 290, return [3]
				return midIndex + 1;
			//		cout << "Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
		}
		return rightIndex; // Key does not appear in indexes, ex: 285, return 2
	}
}

// Find the bucket where 1st matching key belongs
//int searchKeyInIndex(void *data, int numIndex, int key)
//{
//	// searchKeyInIndex returns the index of child pointer, ranging from [0, m]
//	int leftKey, rightKey, midKey;
//	int leftIndex = 0;
//	int rightIndex = numIndex - 1; // numIndex >= 1
//	int offset = 1 + 2*sizeof(int);
//	memcpy(&leftKey, (char *) data + offset, sizeof(int));
//	memcpy(&rightKey, (char *) data + offset + (2*rightIndex)*sizeof(int), sizeof(int));
//	if (key >= rightKey)		// Go down to child[m]
//		return numIndex;
//	else if (key < leftKey)		// Go down to child[0]
//		return 0;
//	else // key[0] <= key < key[m]
//	{
//		int midIndex;
//		while (leftIndex + 1 < rightIndex)
//		{
//			midIndex = (leftIndex + rightIndex) / 2;
//			memcpy(&midKey, (char *) data + offset + (2*midIndex)*sizeof(int), sizeof(int));
//			if (key < midKey)
//				rightIndex = midIndex;
//			else if (key > midKey)
//				leftIndex = midIndex;
//			else // key == midKey, ex: {270, 280, 290, 300}, key = 290, return [3]
//				return midIndex + 1;
//			//		cout << "Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
//		}
//		return rightIndex; // Key does not appear in indexes, ex: 285, return 2
//	}
//}
//
//// searchKeyInIndex returns the index of child pointer, ranging from [0, m]
//int searchKeyInIndex(void *data, int numIndex, float key)
//{
//	float leftKey, rightKey, midKey;
//	int leftIndex = 0;
//	int rightIndex = numIndex - 1; // numIndex >= 1
//	int offset = 1 + 2*sizeof(int);
//	memcpy(&leftKey, (char *) data + offset, sizeof(int));
//	memcpy(&rightKey, (char *) data + offset + (2*rightIndex)*sizeof(int), sizeof(int));
//	if (key >= rightKey)		// Go down to child[m]
//		return numIndex;
//	else if (key < leftKey)		// Go down to child[0]
//		return 0;
//	else // key[0] <= key < key[m]
//	{
//		int midIndex;
//		while (leftIndex + 1 < rightIndex)
//		{
//			midIndex = (leftIndex + rightIndex) / 2;
//			memcpy(&midKey, (char *) data + offset + (2*midIndex)*sizeof(int), sizeof(int));
//			if (key < midKey)
//				rightIndex = midIndex;
//			else if (key > midKey)
//				leftIndex = midIndex;
//			else // key == midKey, ex: {270, 280, 290, 300}, key = 290, return [3]
//				return midIndex + 1;
//			//		cout << "Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
//		}
//		return rightIndex; // Key does not appear in indexes, ex: 285, return 2
//	}
//}



// Search the starting matching key in leaf page, called in scan
// Matched key should reside in this leaf, if empty, read prev or next non-null leaf and return (m-1) or 0
int IX_ScanIterator::searchKeyInLeaf_scan(void *data, int numKeys, int key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
	bool readPrevLeaf;
	bool readNextLeaf;
	if (!isHighKey) // lowKey case
	{
		readNextLeaf = (numKeys == 0) && !isHighKey; // > 285
		while (readNextLeaf)
		{
			int nextLeafId;
			memcpy(&nextLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		readPrevLeaf = (numKeys == 0) && isHighKey;
		while (readPrevLeaf)
		{
			int prevLeafId;
			memcpy(&prevLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageEnd = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}
	// Here leaf is not empty
	int leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numKeys - 1; // numKeys >= 1
	int offset = 1 + 3*sizeof(int); // flag + [m] + prev + next + k1, rid1...
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (numKeys - 1) * 12, sizeof(int));

	if (!highKey) // lowKey case
	{
		// Exceptions: in [280, 290), leaf = {281, 283, 285}, find a key > 285
		// Exceptions: in [280, 290), leaf = {281, 283, 284}, find a key >= 285
		// Case: out of bound, eg: find age > 1000
		// Case: key is in index
		// No Exceptions: in [280, 290), leaf = {280, 281, 285..}, find a key > 280,
		// No Exceptions: in [280, 290), leaf = {280, ..}, find a key >= 280, return key[0]
		readNextLeaf = (!isKeyInIndex) && ((!Inclusive && key >= rightKey) || (Inclusive && key > rightKey));
		if (readNextLeaf)
		{
			int nextLeafId;
			memcpy(&nextLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		// Exceptions: in [280, 290), leaf = {285, 287}, find a key < 285
		// Exceptions: in [280, 290), leaf = {286, 287}, find a key <= 285
		// Case: key is in index
		// No Exceptions: in [270, 280), leaf = {271, 278}, find a key < 280, return key[m-1]
		// Exceptions: in [280, 290), leaf = {281, ..}, find a key <= 280, if leftKey > key, return key[m-1] in previous leaf
		// Case: out of bound, eg: find age < -1
		readPrevLeaf = (!isKeyInIndex) && ((!Inclusive && key <= leftKey) || (Inclusive && key < leftKey ));
		bool readPrevLeaf_case2 = (isKeyInIndex) && (Inclusive && key < leftKey);
		readPrevLeaf = readPrevLeaf || readPrevLeaf_case2;
		if (readPrevLeaf)
		{
			int prevLeafId;
			memcpy(&prevLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageEnd = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}

	// Normal case, here numKeys >= 1
	if (key > rightKey) //
		return (numKeys - 1); // highKey, find the last key that is < or <= 285
	else if (key < leftKey) // eg: find the 1st key <= 285
		return 0;
	else // key[0] <= key <= key[m-1]
	{
		int midIndex;
		bool isKeyInLeaf = false;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + midIndex * 3 * sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else if (key > midKey)
				leftIndex = midIndex;
			else // key == midKey, ex: leaf = {27, 28, 29, 29, 29, 30, 30, 31}
			{
				isKeyInLeaf = true; // Deal with duplicates
				if (!isHighKey && !Inclusive) // Find 1st entry that is > low key, > 29, ->
					leftIndex = midIndex;
				else if (!isHighKey && Inclusive) // Find 1st entry that is >= low key, >= 29, <-
					rightIndex = midIndex;
				else if (isHighKey && !Inclusive) // Find 1st entry that is < high key, < 29, <-
					rightIndex = midIndex;
				else // Find 1st entry that is <= high key, <= 29, ->
					leftIndex = midIndex;
				//			cout << "IX_scan::Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
			}
		}
		if (leftIndex + 1 == rightIndex && Inclusive)
		{
			if (!isHighKey && leftKey == key) // Case: find key >= 29, return leftIndex = 2;
				return leftIndex;
			else if (isHighKey && rightKey == key) // Case: find key <= 29, return rightIndex = 4;
				return rightIndex;
		}
		if (isHighKey)
			return leftIndex;
		else
			return rightIndex;
	}
}


int IX_ScanIterator::searchKeyInLeaf_scan(void *data, int numKeys, float key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
	bool readPrevLeaf;
	bool readNextLeaf;
	if (!isHighKey) // lowKey case
	{
		readNextLeaf = (numKeys == 0) && !isHighKey; // > 285
		while (readNextLeaf)
		{
			int nextLeafId;
			memcpy(&nextLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		readPrevLeaf = (numKeys == 0) && isHighKey;
		while (readPrevLeaf)
		{
			int prevLeafId;
			memcpy(&prevLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageStart = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}
	// Here leaf is not empty
	float leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numKeys - 1; // numKeys >= 1
	int offset = 1 + 3*sizeof(int); // flag + [m] + prev + next + k1, rid1...
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (numKeys - 1) * 12, sizeof(int));

	if (!highKey) // lowKey case
	{
		// Exceptions: in [280, 290), leaf = {281, 283, 285}, find a key > 285
		// Exceptions: in [280, 290), leaf = {281, 283, 284}, find a key >= 285
		// Case: out of bound, eg: find age > 1000
		// Case: key is in index
		// No Exceptions: in [280, 290), leaf = {280, 281, 285..}, find a key > 280,
		// No Exceptions: in [280, 290), leaf = {280, ..}, find a key >= 280, return key[0]
		readNextLeaf = (!isKeyInIndex) && ((!Inclusive && key >= rightKey) || (Inclusive && key > rightKey));
		if (readNextLeaf)
		{
			int nextLeafId;
			memcpy(&nextLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		// Exceptions: in [280, 290), leaf = {285, 287}, find a key < 285
		// Exceptions: in [280, 290), leaf = {286, 287}, find a key <= 285
		// Case: key is in index
		// No Exceptions: in [270, 280), leaf = {271, 278}, find a key < 280, return key[m-1]
		// Exceptions: in [280, 290), leaf = {281, ..}, find a key <= 280, if leftKey > key, return key[m-1] in previous leaf
		// Case: out of bound, eg: find age < -1
		readPrevLeaf = (!isKeyInIndex) && ((!Inclusive && key <= leftKey) || (Inclusive && key < leftKey ));
		bool readPrevLeaf_case2 = (isKeyInIndex) && (Inclusive && key < leftKey);
		readPrevLeaf = readPrevLeaf || readPrevLeaf_case2;
		if (readPrevLeaf)
		{
			int prevLeafId;
			memcpy(&prevLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageStart = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}

	// Normal case, here numKeys >= 1
	if (key > rightKey) //
		return (numKeys - 1); // highKey, find the last key that is < or <= 285
	else if (key < leftKey) // eg: find the 1st key <= 285
		return 0;
	else // key[0] <= key <= key[m-1]
	{
		int midIndex;
		bool isKeyInLeaf = false;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + midIndex * 3 * sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else if (key > midKey)
				leftIndex = midIndex;
			else // key == midKey, ex: leaf = {27, 28, 29, 29, 29, 30, 30, 31}
			{
				isKeyInLeaf = true; // Deal with duplicates
				if (!isHighKey && !Inclusive) // Find 1st entry that is > low key, > 29, ->
					leftIndex = midIndex;
				else if (!isHighKey && Inclusive) // Find 1st entry that is >= low key, >= 29, <-
					rightIndex = midIndex;
				else if (isHighKey && !Inclusive) // Find 1st entry that is < high key, < 29, <-
					rightIndex = midIndex;
				else // Find 1st entry that is <= high key, <= 29, ->
					leftIndex = midIndex;
				//			cout << "IX_scan::Then l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << midKey << endl;
			}
		}
		if (leftIndex + 1 == rightIndex && Inclusive)
		{
			if (!isHighKey && leftKey == key) // Case: find key >= 29, return leftIndex = 2;
				return leftIndex;
			else if (isHighKey && rightKey == key) // Case: find key <= 29, return rightIndex = 4;
				return rightIndex;
		}
		if (isHighKey)
			return leftIndex;
		else
			return rightIndex;
	}
}


// VARCHAR
int IX_ScanIterator::searchKeyInLeaf_scan(void *data, int numKeys, char* key, bool isHighKey, bool Inclusive, bool &isKeyInIndex)
{
	if (isScanLeaf)
		cout << "srch " << *(key+4) << "...";
	bool readPrevLeaf;
	bool readNextLeaf;
	if (!isHighKey) // lowKey case
	{
		readNextLeaf = (numKeys == 0) && !isHighKey; // > 285
		while (readNextLeaf)
		{
			int nextLeafId;
			memcpy(&nextLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		readPrevLeaf = (numKeys == 0) && isHighKey;
		while (readPrevLeaf)
		{
			int prevLeafId;
			memcpy(&prevLeafId, (char *)data + 1 + 2*sizeof(int), sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				memcpy(&numKeys, (char *)data + 1, sizeof(int));
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageStart = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}
	// Here leaf is not empty
	// In case the there are some emtpy slots in slor dir
//	int m = *(int *)((char *)data + 1);
//	int slotCount = *(int *)((char *)data + PAGE_SIZE - 3*sizeof(int));
	vector<int> entryOffsets; // get the entry offsets
	getValidEntryOffsets(data, entryOffsets);


	int leftIndex = 0;
	int rightIndex = numKeys - 1; // numIndex >= 1
	int entryOffset = *(int *)((char *)data + PAGE_SIZE - (leftIndex+1 + 3)*sizeof(int));
	int varLength = *(int *)((char *)data + entryOffset);
	// assert(varLength <= 26 && "searchKeyInIndex() invalid varLength!");
	char *leftKey = (char *)malloc(varLength + 4);
	memcpy(leftKey, (char *)data + entryOffset, 4 + varLength);

	entryOffset = *(int *)((char *)data + PAGE_SIZE - (rightIndex+1 + 3)*sizeof(int));
	varLength = *(int *)((char *)data + entryOffset);
	char *rightKey = (char *)malloc(varLength + 4);
	memcpy(rightKey, (char *)data + entryOffset, 4 + varLength);

	int key_rightKey = strcmp_1(key, rightKey);
	int key_leftKey = strcmp_1(key, leftKey);

	if (!highKey) // lowKey case
	{
		// Exceptions: in [280, 290), leaf = {281, 283, 285}, find a key > 285
		// Exceptions: in [280, 290), leaf = {281, 283, 284}, find a key >= 285
		// Case: out of bound, eg: find age > 1000
		// Case: key is in index
		// No Exceptions: in [280, 290), leaf = {280, 281, 285..}, find a key > 280,
		// No Exceptions: in [280, 290), leaf = {280, ..}, find a key >= 280, return key[0]
		readNextLeaf = (!isKeyInIndex) && ((!Inclusive && key_rightKey >= 0) || (Inclusive && key_rightKey > 0));
		if (readNextLeaf)
		{
			int nextLeafId = *(int *)((char *)data + 1 + 2*sizeof(int));
			if (nextLeafId == -1) // No next page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(nextLeafId, data);
				numKeys = *(int *)((char *)data + 1);
				readNextLeaf = (numKeys == 0) && !isHighKey;
				if (!readNextLeaf)
				{
					this->leafPageStart = nextLeafId; // Update leafPageStart
					return 0;
				}
			}
		}

	}
	else // highKey case
	{
		// Exceptions: in [280, 290), leaf = {285, 287}, find a key < 285
		// Exceptions: in [280, 290), leaf = {286, 287}, find a key <= 285
		// Case: key is in index
		// No Exceptions: in [270, 280), leaf = {271, 278}, find a key < 280, return key[m-1]
		// Exceptions: in [280, 290), leaf = {281, ..}, find a key <= 280, if leftKey > key, return key[m-1] in previous leaf
		// Case: out of bound, eg: find age < -1
		readPrevLeaf = (!isKeyInIndex) && ((!Inclusive && key_leftKey <= 0) || (Inclusive && key_leftKey < 0));
		bool readPrevLeaf_case2 = (isKeyInIndex) && (Inclusive && key_leftKey < 0);
		readPrevLeaf = readPrevLeaf || readPrevLeaf_case2;
		if (readPrevLeaf)
		{
			int prevLeafId = *(int *)((char *)data + 1 + 2*sizeof(int));
			if (prevLeafId == -1) // No previous page any more
				return -1;
			else
			{
				ixfileHandle.fileHandle.readPage(prevLeafId, data);
				numKeys = *(int *)((char *)data + 1);
				readPrevLeaf = (numKeys == 0) && isHighKey;
				if (!readPrevLeaf)
				{
					this->leafPageStart = prevLeafId; // Update leafPageStart
					return (numKeys - 1);
				}
			}
		}
	}
	entryOffsets.clear();
	getValidEntryOffsets(data, entryOffsets);
	// Normal case, here numKeys >= 1
	if (key_rightKey > 0)
	{
		free(leftKey);
		free(rightKey);
		return (numKeys - 1); // highKey, find the last key that is < or <= 285
	}
	else if (key_leftKey < 0) // eg: find the 1st key <= 285
	{
		free(leftKey);
		free(rightKey);
		return 0;
	}
	else // key[0] <= key <= key[m-1]
	{
		bool isKeyInLeaf = false;
		while (leftIndex + 1 < rightIndex)
		{
			int midIndex = (leftIndex + rightIndex) / 2;
			varLength = *(int *)((char *)data + entryOffsets[midIndex]);
			char *midKey = (char *)malloc(varLength + 4);
			memcpy(midKey, (char *)data + entryOffsets[midIndex], 4 + varLength);
			int key_midKey = strcmp_1(key, midKey);
			if (key_midKey < 0)
				rightIndex = midIndex;
			else if (key_midKey > 0)
				leftIndex = midIndex;
			else // key == midKey, ex: leaf = {27, 28, 29, 29, 29, 30, 30, 31}
			{
				isKeyInLeaf = true; // Deal with duplicates
				if (!isHighKey && !Inclusive) // Find 1st entry that is > low key, > 29, ->
					leftIndex = midIndex;
				else if (!isHighKey && Inclusive) // Find 1st entry that is >= low key, >= 29, <-
					rightIndex = midIndex;
				else if (isHighKey && !Inclusive) // Find 1st entry that is < high key, < 29, <-
					rightIndex = midIndex;
				else // Find 1st entry that is <= high key, <= 29, ->
					leftIndex = midIndex;
				if (isScanLeaf)
					cout << "ScanKeyInLeaf: l = " << leftIndex << ", m = " << midIndex << ", r = " << rightIndex << ", mk = " << *midKey << endl;
			}
			free(midKey);
		}
		if (leftIndex + 1 == rightIndex && Inclusive)
		{
			if (!isHighKey && strcmp_1(leftKey, key) == 0) // Case: find key >= 29, return leftIndex = 2;
			{
				free(leftKey);
				free(rightKey);
				return leftIndex;
			}
			else if (isHighKey && strcmp_1(rightKey, key) == 0) // Case: find key <= 29, return rightIndex = 4;
			{
				free(leftKey);
				free(rightKey);
				return rightIndex;
			}
		}
		free(leftKey);
		free(rightKey);
		if (isHighKey)
			return leftIndex;
		else
			return rightIndex;
	}
}



template<class T>
int searchKeyInLeaf(void *data, int numKeys, T key)
{
	// searchLeaf() returns the key index where the inserted key should belong
	// eg:Curleaf {10 20, 30, 40}
	// if insert 5, return [0]
	// if insert 10, return [1]
	// if insert 40/45, return [4]
	if (numKeys == 0) // No keys in leaf, search failed! Insert key at [0]
		return 0;
	T leftKey, rightKey, midKey;
	int leftIndex = 0;
	int rightIndex = numKeys - 1;
	int offset = 1 + 3*sizeof(int); // flag + [m] + prev + next + k1, rid1...
	memcpy(&leftKey, (char *) data + offset, sizeof(int));
	memcpy(&rightKey, (char *) data + offset + (numKeys - 1) * 12, sizeof(int));

	if (key >= rightKey)
		return numKeys;
	else if (key < leftKey)
		return 0;
	else // key[0] <= key < key[m-1]
	{
		int midIndex;
		while (leftIndex + 1 < rightIndex)
		{
			midIndex = (leftIndex + rightIndex) / 2;
			memcpy(&midKey, (char *) data + offset + midIndex * 3 * sizeof(int), sizeof(int));
			if (key < midKey)
				rightIndex = midIndex;
			else // (key >= midKey)
				leftIndex = midIndex;
		}
		return rightIndex;
	}
}

//int searchKeyInLeaf(void *data, int numKeys, int key)
//{
//	// searchLeaf() returns the key index where the inserted key should belong
//	// eg:Curleaf {10 20, 30, 40}
//	// if insert 5, return [0]
//	// if insert 10, return [1]
//	// if insert 40/45, return [4]
//	if (numKeys == 0) // No keys in leaf, search failed! Insert key at [0]
//		return 0;
//	int leftKey, rightKey, midKey;
//	int leftIndex = 0;
//	int rightIndex = numKeys - 1;
//	int offset = 1 + 3*sizeof(int); // flag + [m] + prev + next + k1, rid1...
//	memcpy(&leftKey, (char *) data + offset, sizeof(int));
//	memcpy(&rightKey, (char *) data + offset + (numKeys - 1) * 12, sizeof(int));
//
//	if (key >= rightKey)
//		return numKeys;
//	else if (key < leftKey)
//		return 0;
//	else // key[0] <= key < key[m-1]
//	{
//		int midIndex;
//		while (leftIndex + 1 < rightIndex)
//		{
//			midIndex = (leftIndex + rightIndex) / 2;
//			memcpy(&midKey, (char *) data + offset + midIndex * 3 * sizeof(int), sizeof(int));
//			if (key < midKey)
//				rightIndex = midIndex;
//			else // (key >= midKey)
//				leftIndex = midIndex;
//		}
//		return rightIndex;
//	}
//}
//
//int searchKeyInLeaf(void *data, int numKeys, float key)
//{
//	// searchLeaf() returns the key index where the inserted key should belong
//	// eg:Curleaf {10 20, 30, 40}
//	// if insert 5, return [0]
//	// if insert 10, return [1]
//	// if insert 40/45, return [4]
//	if (numKeys == 0) // No keys in leaf, search failed! Insert key at [0]
//		return 0;
//	float leftKey, rightKey, midKey;
//	int leftIndex = 0;
//	int rightIndex = numKeys - 1;
//	int offset = 1 + 3*sizeof(int); // flag + [m] + prev + next + k1, rid1...
//	memcpy(&leftKey, (char *) data + offset, sizeof(int));
//	memcpy(&rightKey, (char *) data + offset + (numKeys - 1) * 12, sizeof(int));
//
//	if (key >= rightKey)
//		return numKeys;
//	else if (key < leftKey)
//		return 0;
//	else // key[0] <= key < key[m-1]
//	{
//		int midIndex;
//		while (leftIndex + 1 < rightIndex)
//		{
//			midIndex = (leftIndex + rightIndex) / 2;
//			memcpy(&midKey, (char *) data + offset + midIndex * 3 * sizeof(int), sizeof(int));
//			if (key < midKey)
//				rightIndex = midIndex;
//			else // (key >= midKey)
//				leftIndex = midIndex;
//		}
//		return rightIndex;
//	}
//}


IXFileHandle::IXFileHandle()
{
	//	cout << "IXFileHandle constructor...\n";
	ixReadPageCounter = this->fileHandle.readPageCounter;
	ixWritePageCounter = this->fileHandle.writePageCounter;
	ixAppendPageCounter = this->fileHandle.appendPageCounter;
}

IXFileHandle::~IXFileHandle() {
	//	cout << "IXFIleHandle destructor...\n";
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
		unsigned &appendPageCount)
{
	RC rc = this->fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
	return rc;
}

