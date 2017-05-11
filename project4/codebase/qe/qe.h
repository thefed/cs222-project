#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"


#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};

void copyVal(Value value, void *compVal);

struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
		Iterator* it;
		const Condition *cond;
		vector<Attribute> attrs;
		int index; // Index of filtering attr
		void *compVal;
		void *attrVal;

		Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{ it->getAttributes(attrs);};
};

class Project: public Iterator {
// Projection operator
public:
	Iterator* it;
	vector<string> attrNames;
	vector<Attribute> attrs; // Attributes of data from TableScan.getNextTuple()
	vector<Attribute> projAttrs; // Attributes of returned data from Project.getNextTuple()
	vector<int> projAttrIndexes; // Indexes of projected attributes
	void *tmpData;

	Project(Iterator *input, const vector<string> &attrNames);
	// Iterator of input R
	// vector containing attribute names
	~Project();

	RC getNextTuple(void *data);

// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for the undergraduate solo teams: 5 extra-credit points
class BNLJoin: public Iterator {
// Block nested-loop join operator
public:
	Iterator *leftIn;
	TableScan *rightIn;
	const Condition *cond;

	int tuplesSize; // Current buffer size of scanned tuples, bufferSize < numPages * PAGE_SIZE
	int blockSize; // Size allocated for block
	int inputBufferSize; // Size allocated for input buffer

	vector<Attribute> attrs1; // Tuple descriptor of left and right table
	vector<Attribute> attrs2;

	multimap<int, int> mm_int; // multimap for TypeInt <key, tupleId> in left block
	multimap<float, int> mm_float; // multimap for TypeInt <key, tupleId> in left block
	multimap<string, int> mm_str; // multimap for TypeInt <key, tupleId> in left block

	unsigned matchedCount;

	void *inputBuffer;
	void *block; // Block allocated for BNL join
	vector<int> tupleOffsets1; // offset and length vector used in R-block
	vector<int> tupleLengths1;
	vector<int> tupleOffsets2; // offset and length vector used in S-page
	vector<int> tupleLengths2;
	vector<int> matchingIds1; // matching tuples' id in left
	vector<int> matchingIds2; // matching tuples' id in right

//	bool isCreateMap; // Used to mark whether to create map when loading tuples into buffer

	void *tmpTuple1;
	void *tmpTuple2;
	unsigned tupleCount1;
	unsigned tupleCount2;
	bool isRefillBuffer1;
	bool isRefillBuffer2;
	bool isLastTupleVisited1;
	bool isLastTupleVisited2;
	bool isTmpTuple1; // Used to mark whether there is temperate tuple in refilling block
	bool isTmpTuple2; // Used to mark whether there is temperate tuple in refilling input buffer

	void *data1; // returned data from getNextTuple
	void *data2;
	void *value1; // comparing attr value of tuple
	void *value2;
	int index1; // index of comparing attr in attrs
	int index2;
	int bufsize1; // Max size of a tuple
	int bufsize2;

	BNLJoin(Iterator *leftIn,            // Iterator of input R
					TableScan *rightIn,           // TableScan Iterator of input S
					const Condition &condition,   // Join condition
					const unsigned numPages       // # of pages that can be loaded into memory,
												  //   i.e., memory block size (decided by the optimizer)
					);
	~BNLJoin();

	RC getNextTuple(void *data);
// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

	RC loadTuplesIntoBuffer(Iterator *input, bool &isLastTupleVisited, bool &isTmpTuple,
			const vector<Attribute> &attrs, const int &bufferSize, void *tmpTuple,
			void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, bool isOuter);

	void getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
			multimap<int, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2);

	void getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
			multimap<float, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2);

	void getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
			multimap<string, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2);

	template<class T>
	void createHash(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, multimap<T, int> &mm);

	void createHash(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, multimap<string, int> &mm);

};
class INLJoin: public Iterator {
// Index nested-loop join operator
public:
	Iterator *leftIn;
	IndexScan *rightIn;
	const Condition *cond;

	vector<Attribute> attrs1; // Tuple descriptor of left and right table
	vector<Attribute> attrs2;
	void *data1; // returned data from getNextTuple
	void *data2;
	void *value1; // comparing attr value of tuple
	void *value2;
	int index1; // index of comparing attr in attrs
	int index2;
	int bufsize1; // Max size of a tuple
	int bufsize2;

	INLJoin(Iterator *leftIn,           // Iterator of input R
					IndexScan *rightIn,          // IndexScan Iterator of input S
					const Condition &condition   // Join condition
					);
	~INLJoin();

	RC getNextTuple(void *data);
// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for everyone. 10 extra-credit points
class GHJoin: public Iterator {
// Grace hash join operator
public:

	Iterator *leftIn;
	Iterator *rightIn;
	const Condition *cond;
	unsigned numPart; // Also used as size allocated for input buffer

	Condition cond2; // Used by getNextTuple()

	RelationManager *rm;
	TableScan *pleftIn;
	TableScan *prightIn;
	BNLJoin *bnlJoin;

	unsigned curPartId;
	unsigned lastPartId;

	vector<Attribute> attrs1; // Tuple descriptor of left and right table
	vector<Attribute> attrs2;
	int index1; // index of comparing attr in attrs
	int index2;

	GHJoin(Iterator *leftIn,               // Iterator of input R
					Iterator *rightIn,               // Iterator of input S
					const Condition &condition,      // Join condition (CompOp is always EQ)
					const unsigned numPartitions // # of partitions for each relation (decided by the optimizer)
					);
	~GHJoin();

	RC getNextTuple(void *data);

// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

class Aggregate: public Iterator {
// Aggregation operator
public:
	Iterator *it;
	Attribute aggAttr;
	Attribute gAttr;
	AggregateOp op;

	vector<Attribute> attrs;
	int index; // Index of filtering attr
	int bufsize;
	void *tmpData; // Returned data from input->getNextTuple
	void *attrVal; // Value fetched from current tuple
	float sum;
	float avg;
	int tupleCount; // Number of tuples scanned

	int opCount; // For aggregate operation, only return 0 once

	// Data members used by group by attribute
	RelationManager *rm;

	bool hasGroupBy;
	int curGroupId;
	int numGroups;
	int gIndex;
	map<int, int>map_int;
	map<float, int>map_float;
	map<string, int>map_str;
	Attribute aggAttr_g;
	Attribute gAttr_g; // Used by getNextTuple()

// Mandatory for graduate teams/solos. Optional for undergrad solo teams: 5 extra-credit points
// Basic aggregation
	Aggregate(Iterator *input,          // Iterator of input R
					Attribute aggAttr,     // The attribute over which we are computing an aggregate
					AggregateOp op            // Aggregate operation
					);

// Optional for everyone: 5 extra-credit points
// Group-based hash aggregation
	Aggregate(Iterator *input,             // Iterator of input R
					Attribute aggAttr,     // The attribute over which we are computing an aggregate
					Attribute groupAttr,      // The attribute over which we are grouping the tuples
					AggregateOp op              // Aggregate operation
					);
	~Aggregate();

	RC getNextTuple(void *data);

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
	void getAttributes(vector<Attribute> &attrs) const;

	RC getTuple(void *data);

	RC groupByAttr(Iterator *input, Attribute groupAttr, const vector<Attribute> attrs, int index, int &numGroups);
};

#endif
