#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "../rbf/rbfm.h"

using namespace std;

#define SYS 0               // db system admin ID
#define USER 1              // user ID
#define CAT_RECORD_SIZE 100 // catalog record size
# define RM_EOF (-1)        // end of a scan operator

// const char *TABLES = "Tables";     // file name of schema table
// const char *COLUMNS = "Columns";   // file name of schema column
const string TABLES = "Tables";
const string COLUMNS = "Columns";


// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
    public:
        RBFM_ScanIterator rbfmsi;
        RM_ScanIterator();
        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);
        RC close();
};


// Relation Manager
class RelationManager
{
    public:
        static RelationManager* instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const string &tableName, const vector<Attribute> &attrs);

        RC deleteTable(const string &tableName);

        RC getAttributes(const string &tableName, vector<Attribute> &attrs);

        RC insertTuple(const string &tableName, const void *data, RID &rid);

        RC deleteTuple(const string &tableName, const RID &rid);

        RC updateTuple(const string &tableName, const void *data, const RID &rid);

        RC readTuple(const string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const vector<Attribute> &attrs, const void *data);

        RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const string &tableName,
                const string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const vector<string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
    public:
        RC addAttribute(const string &tableName, const Attribute &attr);

        RC dropAttribute(const string &tableName, const string &attributeName);


    protected:
        RelationManager();
        ~RelationManager();

    private:
        static RelationManager *_rm;
        RecordBasedFileManager *rbf;    // access RecordBasedFileManager's APIs
};

#endif
