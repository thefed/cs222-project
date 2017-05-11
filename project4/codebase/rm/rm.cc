#include "rm.h"

RelationManager* RelationManager::_rm = 0;

bool isShowCrtTbl = false;
bool isShowDelTbl = false;

inline string getIndexFileName(string table_name, string attr_name) {
	return table_name + "_" + attr_name;
}

void printTables()
{
	cout << "***** Tables info *****\n";
	string tableName = TABLES;
	vector<Attribute> attrs;
	RelationManager *rm = RelationManager::instance();
	RC rc = rm->getAttributes(tableName, attrs);
	assert(rc == 0 && "RelationManager::getAttributes() should not fail.");
	RM_ScanIterator rmsi;
	vector<string> projected_attrs;
	for (size_t i = 0; i < attrs.size(); i++)
		projected_attrs.push_back(attrs[i].name);
	rc = rm->scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
	assert(rc == 0 && "RelationManager::scan() should not fail.");
	void *returnedData = malloc(200);
	RID rid;
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		rm->printTuple(attrs, returnedData);
	}
	rmsi.close();
	free(returnedData);
	cout << "***** Tables end *****\n";
}

void createTablesDescriptor(vector<Attribute> &tablesDescriptor)
{
	tablesDescriptor.clear();

	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	tablesDescriptor.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	tablesDescriptor.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	tablesDescriptor.push_back(attr);

	attr.name = "authority";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	tablesDescriptor.push_back(attr);
}

void prepareTablesTuple(const int tableId, const int tableNameLength, const string &tableName, const int fileNameLength, const string &fileName,
		const int authority, void *buffer)
{

	unsigned char *nullsIndicator = (unsigned char *) malloc(1);
	memset(nullsIndicator, 0, 1);
	nullsIndicator[0] = 0; // 00000000

	int offset = 0;
	memcpy((char *) buffer + offset, nullsIndicator, 1);
	offset += 1;
	memcpy((char *) buffer + offset, &tableId, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, &tableNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, tableName.c_str(), tableNameLength);
	offset += tableNameLength;
	memcpy((char *) buffer + offset, &fileNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, fileName.c_str(), fileNameLength);
	offset += fileNameLength;
	memcpy((char *) buffer + offset, &authority, sizeof(int));
	offset += sizeof(int);
	free(nullsIndicator);
}

void createColumnsDescriptor(vector<Attribute> &columnsDescriptor)
{
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnsDescriptor.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	columnsDescriptor.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnsDescriptor.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnsDescriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	columnsDescriptor.push_back(attr);
}

void prepareColumnsTuple(const int tableId, const int columnNameLength, const string &columnName, const int columnType, const int columnLength,
		const int columnPosition, void *buffer)
{
	unsigned char *nullsIndicator = (unsigned char *) malloc(1);
	memset(nullsIndicator, 0, 1);
	nullsIndicator[0] = 0; // 00000000
	// int fieldCount = 5; // All non-NULL
	int offset = 0;
	memcpy((char *) buffer + offset, nullsIndicator, 1);
	offset += 1;
	memcpy((char *) buffer + offset, &tableId, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, &columnNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, columnName.c_str(), columnNameLength);
	offset += columnNameLength;
	memcpy((char *) buffer + offset, &columnType, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, &columnLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char *) buffer + offset, &columnPosition, sizeof(int));
	offset += sizeof(int);
	free(nullsIndicator);
}

RelationManager* RelationManager::instance()
{
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	// Create file "Tables"
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbf->createFile(TABLES);
	rbf->openFile(TABLES, fileHandle);

	void *tablesInfo = malloc(TABLE_INFO_SIZE);
	RID rid;
	vector<Attribute> tablesDescriptor;
	createTablesDescriptor(tablesDescriptor);
	// 1, "Tables", "Tables", 0,
	prepareTablesTuple(1, 6, TABLES, 6, TABLES, SYS, tablesInfo);
	rbf->insertRecord(fileHandle, tablesDescriptor, tablesInfo, rid);

	//	(chars include length before them)
	prepareTablesTuple(2, 7, COLUMNS, 7, COLUMNS, SYS, tablesInfo);
	rbf->insertRecord(fileHandle, tablesDescriptor, tablesInfo, rid);

	rbf->closeFile(fileHandle);

	// Create file "Columns"
	rbf->createFile(COLUMNS);
	rbf->openFile(COLUMNS, fileHandle);

	void *columnsInfo = malloc(TABLE_INFO_SIZE);
	vector<Attribute> columnsDescriptor;
	createColumnsDescriptor(columnsDescriptor);

	// (1, "table-id", TypeInt, 4, 1)
	string columnName = "table-id";
	prepareColumnsTuple(1, columnName.size(), columnName, TypeInt, 4, 1, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (1, "table-name", TypeVarChar, 50, 2)
	columnName = "table-name";
	prepareColumnsTuple(1, columnName.size(), columnName, TypeVarChar, 50, 2, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (1, "file-name", TypeVarChar, 50, 3)
	columnName = "file-name";
	prepareColumnsTuple(1, columnName.size(), columnName, TypeVarChar, 50, 3, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (1, "authority", TypeInt, 4, 4)
	columnName = "authority";
	prepareColumnsTuple(1, columnName.size(), columnName, TypeInt, 4, 4, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (2, "table-id", TypeInt, 4, 1)
	columnName = "table-id";
	prepareColumnsTuple(2, columnName.size(), columnName, TypeInt, 4, 1, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (2, "column-name",  TypeVarChar, 50, 2)
	columnName = "column-name";
	prepareColumnsTuple(2, columnName.size(), columnName, TypeVarChar, 50, 2, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (2, "column-type", TypeInt, 4, 3)
	columnName = "column-type";
	prepareColumnsTuple(2, columnName.size(), columnName, TypeInt, 4, 3, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (2, "column-length", TypeInt, 4, 4)
	columnName = "column-length";
	prepareColumnsTuple(2, columnName.size(), columnName, TypeInt, 4, 4, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);

	// (2, "column-position", TypeInt, 4, 5)
	columnName = "column-position";
	prepareColumnsTuple(2, columnName.size(), columnName, TypeInt, 4, 5, columnsInfo);
	rbf->insertRecord(fileHandle, columnsDescriptor, columnsInfo, rid);
	rbf->closeFile(fileHandle);

	free(tablesInfo);
	free(columnsInfo);
	return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rbf->destroyFile(COLUMNS);
	rbf->destroyFile(TABLES);
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC rc;
	// find tableName in "Tables" table
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	RID rid;
	RBFM_ScanIterator rbfm_ScanIterator; //
	rc = rbf->openFile("Tables", rbfm_ScanIterator.fileHandle); // Open Tables
	assert(rc == 0);

	vector<Attribute> tablesDescriptor; //
	createTablesDescriptor(tablesDescriptor);

	vector<string> attributes;
	string attr = "table-id";
	attributes.push_back(attr);
	void *returnedData = malloc(100);
	string condAttr = "table-name";

	int nameLength = (int) tableName.size();
	void *value = malloc(nameLength + sizeof(int));
	memcpy((char *) value, &nameLength, sizeof(int));
	memcpy((char *) value + sizeof(int), tableName.c_str(), nameLength);

	rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, value, attributes, rbfm_ScanIterator);
	assert(rc == 0);

	while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF)
	{
		rbfm_ScanIterator.close(); // If table not found, close Tables
		free(returnedData);
		cout << "Table(" << tableName << ") already exists in Tables, create table failed!\n";
		return -1; // Table already exists, return failure!
	}
	free(value);
	free(returnedData);

	// Write Tables info to "Tables" file
	int tablesPageNum = rbfm_ScanIterator.fileHandle.getNumberOfPages();
	void* curPageData = malloc(PAGE_SIZE);
	rbfm_ScanIterator.fileHandle.readPage(tablesPageNum - 1, curPageData);

	// calculate table ID to be assigned to current record
	short slotCount = getSlotCount(curPageData);
	// memcpy(&slotCount, (char *) curPageData + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
	int validTableIdCount = 0;
	short tupleLength;
	for (int i = 0; i < slotCount; i++)
	{
		tupleLength = getRecordLength(curPageData, i);
		// memcpy(&tupleLength, (char *) curPageData + PAGE_SIZE - (2 * i + 3) * sizeof(short), sizeof(short));
		if (tupleLength > 0)
			validTableIdCount++;
		// else < 0, table deleted, a negative id is left
	}
	validTableIdCount++;
	free(curPageData);

	// tablesInfo: table information buffer(tuple to be inserted in "Tables" table)
	void *tablesInfo = malloc(TABLE_INFO_SIZE);

	prepareTablesTuple(validTableIdCount, tableName.size(), tableName, tableName.size(), tableName, SYS, tablesInfo);
	// Print table info here
	if (isShowCrtTbl)
	{
		cout << "Creating ";
		rc = printTuple(tablesDescriptor, tablesInfo);
		assert(rc == 0);
	}
	rc = rbf->insertRecord(rbfm_ScanIterator.fileHandle, tablesDescriptor, tablesInfo, rid);
	assert(rc == 0);

	free(tablesInfo);
	rc = rbfm_ScanIterator.close();
	assert(rc == 0);

	// Write Columns info to "Columns" file
	rc = rbf->openFile(COLUMNS, rbfm_ScanIterator.fileHandle);
	assert(rc == 0);

	void *columnsInfo = malloc(TABLE_INFO_SIZE);
	vector<Attribute> columnsDescriptor;
	createColumnsDescriptor(columnsDescriptor);
	for (unsigned i = 0; i < attrs.size(); i++)
	{
		string columnName = attrs[i].name;
		AttrType columnType = attrs[i].type;
		int columnLength = attrs[i].length;
		prepareColumnsTuple(validTableIdCount, columnName.size(), columnName, columnType, columnLength, i + 1, columnsInfo);
//				printTuple(columnsDescriptor,columnsInfo);
		rc = rbf->insertRecord(rbfm_ScanIterator.fileHandle, columnsDescriptor, columnsInfo, rid);
		assert(rc == 0);
	}
	free(columnsInfo);
	rc = rbf->closeFile(rbfm_ScanIterator.fileHandle);
	assert(rc == 0);

	// Create new file for this table
	rc = rbf->createFile(tableName);
	assert(rc == 0);
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RC rc;
	bool invalidDeletion = tableName.empty() || tableName == "Tables" || tableName == "Columns";
	if (invalidDeletion) // User cannot delete system table
	{
		assert(!invalidDeletion && "User cannot delete system table");
		return -1;
	}
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	if (rbf->destroyFile(tableName) == -1)
		return -1;
	if (isShowDelTbl)
		cout << "Delete table(" << tableName << ") in Tables...\n";

	// If this table has index files, delete them too
	vector<string> deleteTableNames;
	deleteTableNames.push_back(tableName);
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) == -1)
		return -1;
	for (unsigned i = 0; i < attrs.size(); i++)
	{
		string indexFileName = getIndexFileName(tableName, attrs[i].name);
		deleteTableNames.push_back(indexFileName);
	}

	// delete tuple in "Tables"

	vector<Attribute> tablesDescriptor; //
	createTablesDescriptor(tablesDescriptor);

	vector<string> attributesTable; //
	string attrTable = "table-id";
	attributesTable.push_back(attrTable);

	for (unsigned i = 0; i < attrs.size() + 1; i++) // Loop from table-name to tablename_attr[0]-[n-1]
	{
		rc = rbf->destroyFile(deleteTableNames[i]); // Remove file first, then clear catalog info
		assert(rc == 0);
		int nameLength = (int) deleteTableNames[i].size();
		void *value = malloc(nameLength + sizeof(int));
		memcpy((char *) value, &nameLength, sizeof(int));
		memcpy((char *) value + sizeof(int), deleteTableNames[i].c_str(), nameLength);

		// Start scan
		RBFM_ScanIterator rbfm_ScanIterator;
		rc = rbf->openFile(TABLES, rbfm_ScanIterator.fileHandle);
		assert(rc == 0);
		int rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, "table-name", EQ_OP, value, attributesTable, rbfm_ScanIterator);
		if (rc == -1)
		{
			cout << TABLES << " open failed in RM::deleteTable()\n";
			return -1;
		}
		void *returnedData = malloc(attrs[i].length + 5);
		RID rid;
		bool isTableExist = false;
		while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF)
		{
			rc = rbf->deleteRecord(rbfm_ScanIterator.fileHandle, tablesDescriptor, rid);
			assert(rc == 0);
			isTableExist = true;
			break;
		}
		rbfm_ScanIterator.close();
		free(value);
		if (!isTableExist) // The table to be deleted does not exist! delete table failed.
		{
			free(returnedData);
			if (i == 0) // Table does not exist, it's OK that some index files do not exist
			{
				cout << tableName << " does not exist in Tables file! Delete table failed.\n";
				return -1;
			}
		}	
		else			// found the table to be deleted
		{
			int deletedTableId = *(int *) ((char *) returnedData + 1);

			// delete tuple in "Columns" file
			// Rebind scan
			rbf->openFile(COLUMNS, rbfm_ScanIterator.fileHandle);
			vector<Attribute> columnsDescriptor;
			createColumnsDescriptor(columnsDescriptor);
			vector<string> attributesColumn; //
			string condAttr = "table-id";
			attributesColumn.push_back(condAttr);

			rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, &deletedTableId, attributesColumn, rbfm_ScanIterator);
			assert(rc == 0);

			while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF)
			{
				rc = rbf->deleteRecord(rbfm_ScanIterator.fileHandle, columnsDescriptor, rid);
				assert(rc == 0);
			}
			rc = rbfm_ScanIterator.close();
			assert(rc == 0);

			if (isShowDelTbl)
				cout << "Delete table(" << deleteTableNames[i] << ") success! Deleted table ID = " << deletedTableId << "\n";
			free(returnedData);
		}
	}
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// Check if tableName is valid and exists
	if (tableName.empty()) return -1;
	if (tableName == "Tables")
	{
		createTablesDescriptor(attrs);
		return 0;
	}
	else if (tableName == "Columns")
	{
		createColumnsDescriptor(attrs);
		return 0;
	}
	else
	{
		// 1. Get table-id from "Tables" file
		RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
		RBFM_ScanIterator rbfm_ScanIterator;

		vector<Attribute> tablesDescriptor; //
		createTablesDescriptor(tablesDescriptor);

		RC rc = rbf->openFile(TABLES, rbfm_ScanIterator.fileHandle);
		assert(rc == 0 && "RelationManager::getAttributes() failed, reopen TABLES is denied!");

		vector<string> attributesTable; //
		string attr = "table-id";
		attributesTable.push_back(attr);
		string condAttr = "table-name";
		RID rid;
		void *returnedData = malloc(100);
		bool tableExist = false;

		int nameLength = (int) tableName.size();
		void *value = malloc(nameLength + sizeof(int));
		memcpy((char *) value, &nameLength, sizeof(int));
		memcpy((char *) value + sizeof(int), tableName.c_str(), nameLength);

		rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, value, attributesTable, rbfm_ScanIterator);
		assert(rc == 0);

		while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != -1)
		{
			tableExist = true;
			break;
		}
		rc = rbfm_ScanIterator.close();
		assert(rc == 0);
		free(value);
		if (!tableExist)
		{
//			cout << "RM::getAttributes() failed, table(" << tableName << ") not found in Tables!\n";
			free(returnedData);
			return -1;
		}

		// 2. Delete columnsInfo in "Columns"
		RBFM_ScanIterator rbfm_ScanIterator2;
		int tableID = *(int *)((char *) returnedData + 1);
		rc = rbf->openFile(COLUMNS, rbfm_ScanIterator2.fileHandle);
		assert(rc == 0);

		vector<Attribute> columnsDescriptor;
		createColumnsDescriptor(columnsDescriptor);

		vector<string> attributesColumn = {"column-name", "column-type", "column-length", "table-id"};
                condAttr = "table-id";  // look for table-id's matching record
		rc = rbf->scan(rbfm_ScanIterator2.fileHandle, columnsDescriptor, condAttr, EQ_OP, &tableID, attributesColumn, rbfm_ScanIterator2);
		assert(rc == 0);

		while (rbfm_ScanIterator2.getNextRecord(rid, returnedData) != RBFM_EOF)
		{
			Attribute attribute;
			int offset = 1;
			// Get column-name
			int nameLength;
			memcpy(&nameLength, (char *) returnedData + 1, sizeof(int));
			offset += sizeof(int);
			char *name = (char *) malloc(nameLength + 1);
			memcpy(name, (char *) returnedData + offset, nameLength);
			offset += nameLength;
			name[nameLength] = '\0';
			attribute.name = name;
			free(name);
			// Get column-type
			memcpy(&attribute.type, (char *) returnedData + offset, sizeof(int));
			offset += sizeof(int);
			// Get column-length
			memcpy(&attribute.length, (char *) returnedData + offset, sizeof(int));
			attrs.push_back(attribute);
		}
		rc = rbfm_ScanIterator2.close();
		assert(rc == 0);
		free(returnedData);
		return 0;
	}
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	// User cannot modify system catalog, return 'failed'
	if (tableName == TABLES || tableName == COLUMNS) return -1;
	RC rc;
	vector<Attribute> tableDescriptor;
	if (getAttributes(tableName, tableDescriptor) == -1) return -1;

	FileHandle fileHandle;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rc = rbf->openFile(tableName, fileHandle);
	assert(rc == 0);
	rc = rbf->insertRecord(fileHandle, tableDescriptor, data, rid);
	assert(rc == 0);
	rc = rbf->closeFile(fileHandle);
	assert(rc == 0);

	// update index file
	IXFileHandle ixfileHandle;
	IndexManager* ix = IndexManager::instance();

	for (size_t i = 0; i < tableDescriptor.size(); i++)
	{
		string indexFileName = getIndexFileName(tableName, tableDescriptor[i].name);
		RBFM_ScanIterator rbfm_ScanIterator;
		vector<Attribute> indexAttrs;
		if (getAttributes(indexFileName, indexAttrs) == 0) // Index file exists, update!
		{
			//			cout << indexFileName << ": ";
			void *key = malloc(indexAttrs[0].length + 4);
			rc = readAttribute(tableName, rid, indexAttrs[0].name, key);
			assert(rc == 0);
			rc = ix->openFile(indexFileName, ixfileHandle);
			assert(rc == 0);
			//			cout << "Inserting key: ";
			//			printKey((char *)key + 1, indexAttrs[0]);
			rc = ix->insertEntry(ixfileHandle, indexAttrs[0], (char *) key + 1, rid);
			assert(rc == 0);
			//			ix->printBtree(ixfileHandle, indexAttrs[0]);
			rc = ix->closeFile(ixfileHandle);
			assert(rc == 0);
			free(key);
		}
	}
	return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if (tableName == TABLES || tableName == COLUMNS) return -1;
	RC rc;
	vector<Attribute> tupleDescriptor;
	rc = getAttributes(tableName, tupleDescriptor);
	if (rc == -1)
		return rc;
	FileHandle fileHandle;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rc = rbf->openFile(tableName, fileHandle);
	assert(rc == 0);
	rc = rbf->deleteRecord(fileHandle, tupleDescriptor, rid);
	assert(rc == 0);
	rc = rbf->closeFile(fileHandle);
	assert(rc == 0);
	return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if (tableName == TABLES || tableName == COLUMNS)
		return -1;
	RC rc;
	vector<Attribute> tupleDescriptor;
	rc = getAttributes(tableName, tupleDescriptor);
	if (rc == -1)
		return rc;
	FileHandle fileHandle;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rc = rbf->openFile(tableName, fileHandle);
	assert(rc == 0);

	rc = rbf->updateRecord(fileHandle, tupleDescriptor, data, rid);
	assert(rc == 0);
	
	rc = rbf->closeFile(fileHandle);
	assert(rc == 0);
	return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RC rc;
	vector<Attribute> tupleDescriptor;
	rc = getAttributes(tableName, tupleDescriptor);
	if (rc == -1)
		return rc;
	FileHandle fileHandle;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rc = rbf->openFile(tableName, fileHandle);
	assert(rc == 0);

	rc = rc = rbf->readRecord(fileHandle, tupleDescriptor, rid, data);
	assert(rc == 0);

	rc = rbf->closeFile(fileHandle);
	assert(rc == 0);
	return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RC rc;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rc = rbf->printRecord(attrs, data);
	return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RC rc;
	FileHandle fileHandle;
	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	rbf->openFile(tableName, fileHandle);
	vector<Attribute> tupleDescriptor;
	rc = getAttributes(tableName, tupleDescriptor);
	if (rc == -1)
	{
		cout << "getAttr() in RM::readAttr() failed!\n";
		return rc;
	}
	rc = rbf->readAttribute(fileHandle, tupleDescriptor, rid, attributeName, data);
	if (rc == -1)
	{
		cout << "RBFM::readAttr() failed!\n";
		return rc;
	}
	rc = rbf->closeFile(fileHandle);
	assert(rc == 0);
	return rc;
}

RC RelationManager::scan(const string &tableName, const string &conditionAttribute, const CompOp compOp, const void *value,
		const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
{
	RC rc;
	vector<Attribute> recordDescriptor;
	if (getAttributes(tableName, recordDescriptor) == -1)
	{
		cout << "getAttribute(" << tableName << ") failed in RM::scan!\n";
		return -1;
	}

//	cout << "RM_SI initialized, compVal = ..\n";
//	printKey(value, recordDescriptor[0]);

	RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
	// scan the table
	rc = rbf->openFile(tableName, rm_ScanIterator.rbfmsi.fileHandle);
	assert(rc == 0);

	rc = rbf->scan(rm_ScanIterator.rbfmsi.fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmsi);
	assert(rc == 0);

	// Close tableName file when finishing scan all pages, getNextTuple = -1;
	return 0;
}

RM_ScanIterator::RM_ScanIterator()
{
}


RM_ScanIterator::~RM_ScanIterator()
{
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	return rbfmsi.getNextRecord(rid, data);
//	if (rbfmsi.getNextRecord(rid, data) == -1) // close tableName file outside of while loop
//		return -1;
//	else // Scan not finished, continue!
//	{
//		rbfmsi.pageNum = rid.pageNum;
//		rbfmsi.slotNum = rid.slotNum + 1;
//		return 0;
//	}
}

RC RM_ScanIterator::close()
{
	return rbfmsi.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) == -1) // table does not exist
		return -1;
	vector<Attribute> indexAttrs; // get Attribute according to attributeName
	for (unsigned i = 0; i < attrs.size(); i++)
	{
		if (attrs[i].name == attributeName)
		{
			indexAttrs.push_back(attrs[i]); // "A"
			break;
		}
	}
	// create index file
	string indexFileName = getIndexFileName(tableName, attributeName);
	RC rc = createTable(indexFileName, indexAttrs); // filename = "left_A"
	assert(rc == 0 && "RelationManager::createIndex(), createTable() failed!");

	// Scan current table to insert entry to index file
	RM_ScanIterator rmsi;
	vector<string> indexAttrNames;
	indexAttrNames.push_back(attributeName);
	rc = scan(tableName, "", NO_OP, NULL, indexAttrNames, rmsi);
	assert(rc == 0 && "RelationManager::scan() should not fail.");

	void *returnedData = malloc(indexAttrs[0].length + sizeof(int));
	RID rid;

	IndexManager *im = IndexManager::instance();
	IXFileHandle ixfileHandle;
	rc = im->openFile(indexFileName, ixfileHandle);
	assert(rc == 0 && "indexManager::openFile() should not fail.");

	//	cout << "Inserted keys: "; // For left_B, it should be empty
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		//		printKey((char *)returnedData + 1, indexAttrs[0]);
		rc = im->insertEntry(ixfileHandle, indexAttrs[0], (char *) returnedData + 1, rid);
		assert(rc == 0 && "indexManager::insertEntry() should not fail.");
	}

	//	cout << "\nPrint BTree of " << tableName << "_" << attributeName << "..\n";
	//	im->printBtree(ixfileHandle, indexAttrs[0]);

	rc = im->closeFile(ixfileHandle);
	assert(rc == 0 && "indexManager::closeFile() should not fail.");

	//	cout << "\nPrint BTree of " << tableName << "_" << attrs[1].name << "..\n";
	//	im->openFile(tableName + "_B", ixfileHandle);
	//	im->printBtree(ixfileHandle, attrs[1]);
	//	im->closeFile(ixfileHandle);
	rc = rmsi.close();
	assert(rc == 0);

	free(returnedData);
	return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = getIndexFileName(tableName, attributeName);
	int rc = deleteTable(indexFileName);
	return rc;
}


RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator)
{
	// 1. check existence of index file for given attributeName
	string indexFileName = getIndexFileName(tableName, attributeName);
	vector<Attribute> attrs;
	if (getAttributes(indexFileName, attrs) == -1)  // index file does not exist
	{
		printf("Index file '%s' does not exist!\n", indexFileName.c_str());
		return -1;
	}
	IndexManager* ix = IndexManager::instance();
	IXFileHandle ixfileHandle;
	RC rc = ix->openFile(indexFileName, ixfileHandle);
	assert(rc == 0);
	
	rc = ix->scan(ixfileHandle, attrs[0], lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixsi);
	assert(rc == 0 && "ix->scan() in RelationManager::indexScan() failed!");
	return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *data)
{
	return ixsi.getNextEntry(rid, data);
}


RC RM_IndexScanIterator::close()
{
	return ixsi.close();
}

