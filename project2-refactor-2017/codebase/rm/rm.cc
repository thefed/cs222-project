#include "rm.h"

RelationManager* RelationManager::_rm = 0;

// utilities
Attribute createAttr(const string name, const AttrType type, const AttrLength length) {
    Attribute attr;
    attr.name = name;
    attr.type = type;
    attr.length = length;
    return attr;
}  

void createTablesDescriptor(vector<Attribute> &descriptor) {
    descriptor.clear();
    descriptor.push_back(createAttr("table-id", TypeInt, 4));
    descriptor.push_back(createAttr("table-name", TypeVarChar, 50));
    descriptor.push_back(createAttr("file-name", TypeVarChar, 50));
    descriptor.push_back(createAttr("authority", TypeInt, 4));
}

void createColumnsDescriptor(vector<Attribute> &descriptor) {
    descriptor.clear();
    descriptor.push_back(createAttr("table-id", TypeInt, 4));
    descriptor.push_back(createAttr("column-name", TypeVarChar, 50));
    descriptor.push_back(createAttr("column-type", TypeInt, 4));
    descriptor.push_back(createAttr("column-length", TypeInt, 4));
    descriptor.push_back(createAttr("column-position", TypeInt, 4));
}

// fill the given buffer data pointer with table/column catalog info
// max buffer length: 1 + 4 * 4 + 2 * strlen <= 100
void prepareTablesTuple(const int tableId, const char *tableName, const char *fileName, int authority, void *buffer) {
    int offset = 0;
    *(char *) buffer = 0x0; // first byte: nullsIndicator
    offset += 1;
    *(int *) ((char *) buffer + offset) = tableId;
    offset += sizeof(int);
    *(int *) ((char *) buffer + offset) = strlen(tableName);
    offset += sizeof(int);
    memcpy((char *) buffer + offset, tableName, strlen(tableName));
    offset += strlen(tableName);
    *(int *) ((char *) buffer + offset) = strlen(fileName);
    offset += sizeof(int);
    memcpy((char *) buffer + offset, fileName, strlen(fileName));
    offset += strlen(fileName);
    *(int *) ((char *) buffer + offset) = authority;
}

void prepareColumnsTuple(const int tableId, const char *columnName,
        const int columnType, const int columnLength, const int columnPosition,
        void *buffer) {
    int offset = 0;
    *(char *) buffer = 0x0;
    offset += 1;
    *(int *) ((char *) buffer + offset) = tableId;
    offset += sizeof(int);
    *(int *) ((char *) buffer + offset) = strlen(columnName);
    offset += sizeof(int);
    memcpy((char *) buffer + offset, columnName, strlen(columnName));
    offset += strlen(columnName);
    *(int *) ((char *) buffer + offset) = columnType;
    offset += sizeof(int);
    *(int *) ((char *) buffer + offset) = columnLength;
    offset += sizeof(int);
    *(int *) ((char *) buffer + offset) = columnPosition;
}


RelationManager* RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();
    return _rm;
}

RelationManager::RelationManager() {
    this->rbf = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager() {}

RC RelationManager::createCatalog() {
    #ifdef DEBUG
    printf("Creating catalog...\n");
    #endif
    // Create file catalog "Tables"
    RC rc;
    FileHandle fileHandle;          // local variable
    if (rbf->createFile(TABLES) == -1) return -1;
    if (rbf->openFile(TABLES, fileHandle) == -1) return -1;

    // 	tablesRecord: data to be inserted, include null indicator& field data
    void *tablesRecord = malloc(CAT_RECORD_SIZE);
    assert(tablesRecord);

    RID rid;
    vector<Attribute> tablesDescriptor;
    createTablesDescriptor(tablesDescriptor);
    // 1 , "Tables" , "Tables" , 0,
    prepareTablesTuple(1, TABLES.c_str(), TABLES.c_str(), SYS, tablesRecord);
    rc = rbf->insertRecord(fileHandle, tablesDescriptor, tablesRecord, rid);
    assert(rc == 0);

    //	(chars include length before them)
    prepareTablesTuple(2, COLUMNS.c_str(), COLUMNS.c_str(), SYS, tablesRecord);
    rc = rbf->insertRecord(fileHandle, tablesDescriptor, tablesRecord, rid);
    assert(rc == 0);

    printf("File '%s' has %d pages, ", TABLES.c_str(), fileHandle.getNumberOfPages());

    free(tablesRecord);
    rc = rbf->closeFile(fileHandle);
    assert(rc == 0);

    // Create file "Columns"
    if (rbf->createFile(COLUMNS) == -1) return -1;
    if (rbf->openFile(COLUMNS, fileHandle) == -1) return -1;

    void *colRecord = malloc(CAT_RECORD_SIZE);  // column record
    assert(colRecord);

    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);

    // (1, "table-id", TypeInt, 4 , 1)
    prepareColumnsTuple(1, "table-id", TypeInt, sizeof(int), 1, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (1, "table-name", TypeVarChar, 50, 2)
    prepareColumnsTuple(1, "table-name", TypeVarChar, 50, 2, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (1, "file-name", TypeVarChar, 50, 3)
    prepareColumnsTuple(1, "file-name", TypeVarChar, 50, 3, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (1, "authority", TypeInt, 4, 4)
    prepareColumnsTuple(1, "authority", TypeInt, sizeof(int), 4, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (2, "table-id", TypeInt, 4, 1)
    prepareColumnsTuple(2, "table-id", TypeInt, 4, 1, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (2, "column-name",  TypeVarChar, 50, 2)
    prepareColumnsTuple(2, "column-name", TypeVarChar, 50, 2, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (2, "column-type", TypeInt, 4, 3)
    prepareColumnsTuple(2, "column-type", TypeInt, 4, 3, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (2, "column-length", TypeInt, 4, 4)
    prepareColumnsTuple(2, "column-length", TypeInt, 4, 4, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    // (2, "column-position", TypeInt, 4, 5)
    prepareColumnsTuple(2, "column-position", TypeInt, 4, 5, colRecord);
    rc = rbf->insertRecord(fileHandle, columnsDescriptor, colRecord, rid);
    assert(rc == 0);

    printf("File '%s' has %d pages.\n", COLUMNS.c_str(), fileHandle.getNumberOfPages());
    free(colRecord);
    rc = rbf->closeFile(fileHandle);
    assert(rc == 0);
    return 0;
}

RC RelationManager::deleteCatalog() {
    #ifdef DEBUG
    printf("Deleting catalog...\n");
    #endif
    rbf->destroyFile(COLUMNS);
    rbf->destroyFile(TABLES);
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    // find tableName in catalog table, return -1 if it exists
    RBFM_ScanIterator rbfm_ScanIterator;
    RC rc = rbf->openFile(TABLES, rbfm_ScanIterator.fileHandle); // Open Tables
    assert(rc == 0 && "Open catalog table should not fail.");
    vector<Attribute> tablesDescriptor;
    createTablesDescriptor(tablesDescriptor);

    vector<string> attributes;	//
    string attr = "table-id";
    attributes.push_back(attr);
    string condAttr = "table-name";

    void *value = malloc(tableName.size() + sizeof(int));
    assert(value);

    *(int *) value = tableName.size();
    memcpy((char *)value + sizeof(int), tableName.c_str(), tableName.size());

    // scan "Tables" catalog to verify the given table does not exist
    rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, value, attributes, rbfm_ScanIterator);
    assert(rc == 0 && "RBFM::scan() should not fail.");
    free(value);

    RID rid;
    void *returnedData = malloc(CAT_RECORD_SIZE);
    assert(returnedData);
    while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF) {
        rbfm_ScanIterator.close(); // If table not found, close Tables
        free(returnedData);
        printf("%s already exists in catalog Tables, create table failed!\n", tableName.c_str());
        return -1; // Table already exists
    }
    free(returnedData);

    // Write Tables info to "Tables" file
    int tablesPageNum = rbfm_ScanIterator.fileHandle.getNumberOfPages();
    void *curPage = malloc(PAGE_SIZE);
    assert(curPage);
    rc = rbfm_ScanIterator.fileHandle.readPage(tablesPageNum - 1, curPage);
    assert(rc == 0 && "Read last page of table catalog should not fail.");

    // calculate table ID to be assigned to current record
    int slotCount = *(int *) ((char *) curPage + PAGE_SIZE - 2 * sizeof(int));
    int validTableIdCount = 0;
    int tupleLength;
    for (int i = 0; i < slotCount; i++) {
        tupleLength = *(int *) ((char *) curPage + PAGE_SIZE - (2 * i + 3) * sizeof(int));
        if (tupleLength > 0) {   // -1 means deleted table record
            validTableIdCount++;
        }
    }
    validTableIdCount++;
    free(curPage);

    // tableRecord: table information buffer(tuple to be inserted in "Tables" table)
    void *tableRecord = malloc(CAT_RECORD_SIZE);
    assert(tableRecord);

    prepareTablesTuple(validTableIdCount, tableName.c_str(), tableName.c_str(), SYS, tableRecord);
    // cout << "Insert to Tables \n";
    printTuple(tablesDescriptor, tableRecord);
    rc = rbf->insertRecord(rbfm_ScanIterator.fileHandle, tablesDescriptor, tableRecord, rid);
    assert(rc == 0);

    free(tableRecord);
    rbfm_ScanIterator.close();

    // Write Columns info to "Columns" file
    rc = rbf->openFile(COLUMNS, rbfm_ScanIterator.fileHandle);
    #ifdef DEBUG
    // cout << rbfm_ScanIterator.fileHandle.pFile << endl;
    #endif
    assert(rc == 0 && "Open columns catalog should not fail.");
    void *colRecord = malloc(CAT_RECORD_SIZE);
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);
    //printf("Insert record to columns catalog...\n");
    for (unsigned i = 0; i < attrs.size(); i++) {
        string columnName = attrs[i].name;
        AttrType columnType = attrs[i].type;
        int columnLength = attrs[i].length;
        prepareColumnsTuple(validTableIdCount, columnName.c_str(), columnType, columnLength, i + 1, colRecord);
        rc = rbf->insertRecord(rbfm_ScanIterator.fileHandle, columnsDescriptor, colRecord, rid);
        assert(rc == 0);
        // rbf->printRecord(columnsDescriptor, colRecord);
    }
    free(colRecord);
    rc = rbfm_ScanIterator.close();
    // rc = rbf->closeFile(rbfm_ScanIterator.fileHandle);
    assert(rc == 0);

    // Create new file for this table
    rc = rbf->createFile(tableName);
    assert(rc == 0);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName) {
    // User cannot delete system table
    if (tableName.empty() || tableName == TABLES || tableName == COLUMNS)    return -1;
    RC rc;
    if (rbf->destroyFile(tableName) == -1) return -1;

    printf("Deleting table '%s' in 'Tables'...\n", tableName.c_str());

    // delete tuple in "Table" table
    RBFM_ScanIterator rbfm_ScanIterator;
    rc = rbf->openFile(TABLES, rbfm_ScanIterator.fileHandle);    // bind file name and file handle
    assert(rc == 0);
    vector<Attribute> tablesDescriptor;
    createTablesDescriptor(tablesDescriptor);

    vector<string> attributesTable;
    string attrTable = "table-id";
    attributesTable.push_back(attrTable);

    void *value = malloc(tableName.size() + sizeof(int));   // table name data pointer
    *(int *) value = tableName.size();
    memcpy((char *)value + sizeof(int), tableName.c_str(), tableName.size());

    // condAttr: table-name, 
    rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, "table-name", EQ_OP, value, attributesTable, rbfm_ScanIterator);
    assert(rc == 0);

    RID rid;
    void *returnedData = malloc(CAT_RECORD_SIZE);
    bool tableExistFlag = false;
    while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF) {
        rc = rbf->deleteRecord(rbfm_ScanIterator.fileHandle, tablesDescriptor, rid);
        assert(rc == 0);
        printf("- deleting page[%d][%d]...\n", rid.pageNum, rid.slotNum);
        tableExistFlag = true;
        break;
    }
    rc = rbfm_ScanIterator.close();
    assert(rc == 0);

    assert(tableExistFlag);
    if (!tableExistFlag) {
        printf("%s does not exist in catalog 'Tables'! Deletion failed.\n", tableName.c_str());
        free(returnedData);
        return -1;
    }
    free(value);

    // cout << "Delete colRecord in Columns...\n";
    int deletedTableId = *(int *) ((char *) returnedData + 1);
    assert(deletedTableId > 1 && " the user can not delete catalog.");

    //	delete tuple in column catalog file
    if (rbf->openFile(COLUMNS, rbfm_ScanIterator.fileHandle) == -1) return -1;
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);
    vector<string> attributesColumn;	//
    string condAttr = "table-id";
    attributesColumn.push_back(condAttr);

    rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, &deletedTableId, attributesColumn, rbfm_ScanIterator);
    assert(rc == 0);
    while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF) {
        rbf->deleteRecord(rbfm_ScanIterator.fileHandle, columnsDescriptor, rid);
        // cout << "- delete page[" << rid.pageNum << "][" << rid.slotNum << "]...\n";
    }
    rc = rbfm_ScanIterator.close();
    assert(rc == 0);
    printf("Delete %s success, table ID %d.\n", tableName.c_str(), deletedTableId);
    free(returnedData);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    // Check if tableName is valid and exists
    if (tableName.empty()) return -1;
    if (tableName == TABLES) {
        createTablesDescriptor(attrs);
        return 0;
    }
    if (tableName == COLUMNS) {
        createColumnsDescriptor(attrs);
        return 0;
    }
    // 1. Get table-id from "Tables" file
    RBFM_ScanIterator rbfm_ScanIterator;
    RC rc = rbf->openFile(TABLES, rbfm_ScanIterator.fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");

    vector<Attribute> tablesDescriptor;		//
    createTablesDescriptor(tablesDescriptor);

    vector<string> attributesTable;	//
    string attr = "table-id";
    attributesTable.push_back(attr);
    string condAttr = "table-name";
    RID rid;
    void *returnedData = malloc(CAT_RECORD_SIZE);
    bool tableExist = false;

    void *value = malloc(tableName.size() + sizeof(int));
    *(int *) value = tableName.size();
    memcpy((char *)value + sizeof(int), tableName.c_str(), tableName.size());

    rc = rbf->scan(rbfm_ScanIterator.fileHandle, tablesDescriptor, condAttr, EQ_OP, value, attributesTable, rbfm_ScanIterator);
    assert(rc == 0 && "rbf->scan should not fail.");
    while (rbfm_ScanIterator.getNextRecord(rid, returnedData) != RBFM_EOF)
    {
        printf("%s is found in tables catalog in page[%u][%u]\n", tableName.c_str(), rid.pageNum, rid.slotNum);
        tableExist = true;
        break;
    }
    rc = rbfm_ScanIterator.close();
    assert(rc == 0);

    free(value);
    if (!tableExist) {
        printf("RM::getAttributes() failed, %s not found in tables catalog!\n", tableName.c_str());
        free(returnedData);
        return -1;
    }

    // 2. Delete colRecord in "Columns"
    RBFM_ScanIterator rbfm_ScanIterator2;
    int tableID = *(int *) ((char *) returnedData + 1);
    rc = rbf->openFile(COLUMNS, rbfm_ScanIterator2.fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);

    vector<string> attributesColumn;	//
    attr = "column-name";
    attributesColumn.push_back(attr);
    attr = "column-type";
    attributesColumn.push_back(attr);
    attr = "column-length";
    attributesColumn.push_back(attr);
    condAttr = "table-id";

    rc = rbf->scan(rbfm_ScanIterator2.fileHandle, columnsDescriptor, condAttr, EQ_OP, &tableID, attributesColumn, rbfm_ScanIterator2);
    assert(rc == 0 && "rbf->scan should not fail.");
    while (rbfm_ScanIterator2.getNextRecord(rid, returnedData) != RBFM_EOF)
    {
        Attribute attribute;
        int offset = 1;
        // Get column-name
        int nameLength = *(int *) ((char *) returnedData + 1);
        offset += sizeof(int);
        char *name = (char *) malloc(nameLength + 1);
        memcpy(name, (char *) returnedData + offset, nameLength);
        offset += nameLength;
        name[nameLength] = '\0';
        attribute.name = name;
        free(name);
        // Get column-type
        attribute.type = (AttrType) *(int *) ((char *) returnedData + offset);
        offset += sizeof(int);
        // Get column-length
        attribute.length = *(int *) ((char *) returnedData + offset);
        attrs.push_back(attribute);
    }
    rc = rbfm_ScanIterator2.close();
    assert(rc == 0);
    free(returnedData);
    return 0;
}

// User cannot modify system catalog
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    if (tableName == TABLES || tableName == COLUMNS)  return -1;
    vector<Attribute> tableDescriptor;
    RC rc = getAttributes(tableName, tableDescriptor);
    assert(rc == 0 && "RM::getAttributes should not fail.");
    FileHandle fileHandle;
    rc = rbf->openFile(tableName, fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    rc = rbf->insertRecord(fileHandle, tableDescriptor, data, rid);
    rbf->closeFile(fileHandle);
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    if (tableName == TABLES || tableName == COLUMNS) return -1;
    vector<Attribute> tupleDescriptor;
    RC rc = getAttributes(tableName, tupleDescriptor);
    assert(rc == 0 && "RM::getAttributes should not fail.");
    FileHandle fileHandle;
    rc = rbf->openFile(tableName, fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    rc = rbf->deleteRecord(fileHandle, tupleDescriptor, rid);
    rbf->closeFile(fileHandle);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    if (tableName == TABLES || tableName == COLUMNS)   return -1;
    vector<Attribute> tupleDescriptor;
    RC rc = getAttributes(tableName, tupleDescriptor);
    assert(rc == 0 && "RM::getAttributes should not fail.");
    FileHandle fileHandle;
    rc = rbf->openFile(tableName, fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    rc = rbf->updateRecord(fileHandle, tupleDescriptor, data, rid);
    rbf->closeFile(fileHandle);
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
    vector<Attribute> tupleDescriptor;
    RC rc = getAttributes(tableName, tupleDescriptor);
    assert(rc == 0 && "RM::readTuple() should not fail.");
    FileHandle fileHandle;
    rc = rbf->openFile(tableName, fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    rc = rbf->readRecord(fileHandle, tupleDescriptor, rid, data);
    rbf->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
    return rbf->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
        const string &attributeName, void *data) {
    FileHandle fileHandle;
    RC rc = rbf->openFile(tableName, fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");
    vector<Attribute> tupleDescriptor;
    if ((rc = getAttributes(tableName, tupleDescriptor)) == -1) return -1;
    rc = rbf->readAttribute(fileHandle, tupleDescriptor, rid, attributeName, data);
    rbf->closeFile(fileHandle);
    assert(rc == 0 && "RBFM::readAttribute() should not fail.");
    return rc;
}

// scan(tableName, attr, GT_OP, &ageVal, attributes, rmsi);
RC RelationManager::scan(const string &tableName, const string &conditionAttribute,
        const CompOp compOp, const void *value, const vector<string> &attributeNames,
        RM_ScanIterator &rm_ScanIterator) {
    // Pass parameters to RBFM_SI;
    vector<Attribute> recordDescriptor;
    RC rc = getAttributes(tableName, recordDescriptor); // Bind tableName and fileHandle
    assert(rc == 0 && "RM::getAttribute should not fail.");

    // open table file and scan it
    rbf->openFile(tableName, rm_ScanIterator.rbfmsi.fileHandle);
    assert(rc == 0 && "rbf->openFile should not fail.");

    rc = rbf->scan(rm_ScanIterator.rbfmsi.fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmsi);
    // Close tableName file when finishing scan all pages, getNextTuple = -1;
    return rc;
}

RM_ScanIterator::RM_ScanIterator() {}

RM_ScanIterator::~RM_ScanIterator() {}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    // close tableName file outside of while loop
    if (this->rbfmsi.getNextRecord(rid, data) == RBFM_EOF)    return RM_EOF;
    //else // Scan not finished, continue!
    this->rbfmsi.pageNum = rid.pageNum;
    this->rbfmsi.slotNum = rid.slotNum + 1;
    return 0;
}

RC RM_ScanIterator::close() {
    return this->rbfmsi.close();
}


// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) {
    return -1;
}
