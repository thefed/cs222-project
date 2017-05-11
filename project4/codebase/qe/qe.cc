#include "qe.h"

bool isShowFilter = false;
bool isShowBNL = false;
bool isShowGH = true;
bool isDebugGH = false;

void copyVal(Value value, void *compVal){
    if (value.type == TypeVarChar)
    {
        int len = *(int *)value.data;
        memcpy((char *)compVal + 1, value.data, sizeof(int) + len);
    }
    else
        memcpy((char *)compVal + 1, value.data, sizeof(int));
}

// Join two tuple if they match the condition of Join
// The returned schema should be the attributes of tuples from leftIn concatenated with the attributes of tuples from rightIn.
// You don't need to remove any duplicate attributes.
// @param: void *data is returned data
void joinTuple(vector<Attribute> &attrs1, vector<Attribute> &attrs2, void *data1, void *data2, void *data)
{
    int size1 = (int) attrs1.size();
    int size2 = (int) attrs2.size();
    int nullsSize1 = getNullsSize(size1);
    int nullsSize2 = getNullsSize(size2);
    int nullsSize = getNullsSize(size1 + size2);

    //int nullsSize1 = ceil((double) size1 / CHAR_BIT); // size of data1's nullsIndicator
    //int nullsSize2 = ceil((double) size2 / CHAR_BIT); // size of data2's nullsIndicator
    //int nullsSize = ceil((double) (size1 + size2) / CHAR_BIT); // size of data's nullsIndicator
    int len1 = getTupleLength(attrs1, data1); // tuple length contains nulls header
    int len2 = getTupleLength(attrs2, data2);

    unsigned char *nullsIndicator2 = (unsigned char *) malloc(nullsSize2); // nullsIndicator of data2
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullsSize); // nullsIndicator of returned data

    memset(nullsIndicator, 0, nullsSize);
    // Copy nulls header of data1
    memcpy((char *) data, (char *) data1, nullsSize1);
    // Copy actual data of data1
    memcpy((char *) data + nullsSize, (char *) data1 + nullsSize1, len1 - nullsSize1);

    // Get the nulls header from data 2
    memcpy(nullsIndicator2, (char *) data2, nullsSize2);
    for (int i = size1; i < size1 + size2; i++)
    {
        int byte = (int) floor((double) (i - size1) / CHAR_BIT); // current byte
        int bit = (i - size1) % CHAR_BIT;
        int rByte = (int) floor((double) i / CHAR_BIT); // current byte
        int rBit = i % CHAR_BIT; // current bit
        bool nullBit = nullsIndicator2[byte] & (1 << (7 - bit));
        nullsIndicator[rByte] |= (nullBit << (7 - rBit)); // assign current attribute's null bit
    }
    // Copy actual data of data2
    memcpy((char *) data + nullsSize + len1 - nullsSize1, (char *)data2 + nullsSize2, len2 - nullsSize2);

    free(nullsIndicator);
    free(nullsIndicator2);
}

Filter::Filter(Iterator* input, const Condition &condition)
{
    it = input;
    cond = &condition;
    it->getAttributes(attrs);
    //	print(attrs, "Filter.attrs");
    index = getAttrIndex(attrs, cond->lhsAttr);
    compVal = malloc(attrs[index].length + 5);
    attrVal = malloc(attrs[index].length + 5);
}

Filter::~Filter()
{
    it = NULL;
    free(compVal);
    free(attrVal);
}

RC Filter::getNextTuple(void *data)
{
    if (cond->bRhsIsAttr)  return -1;   // compVal
    while(it->getNextTuple(data) != -1) // Full data returned
    {
        readAttrFromTuple(attrs, cond->lhsAttr, data, attrVal);// Parse data
        *(unsigned char *)compVal = 0;
        copyVal(cond->rhsValue, compVal);
        if (isShowFilter)
        {
            printKey((char *)attrVal + 1, attrs[index].type);
            printKey((char *)compVal + 1, attrs[index].type);
        }
        if (isMatch(attrs[index].type, attrVal, compVal, cond->op))
        {
            if (isShowFilter)   printf("Matched!\n");
            return 0;
        }
    }
    return -1;
}

// Project
Project::Project(Iterator *input, const vector<string> &attrNames)
{
    this->it = input;
    this->attrNames = attrNames;
    it->getAttributes(this->attrs); // attrs of data from TableScan

    // Get projected attributes and indexes
    for (size_t i = 0; i < attrNames.size(); i++)
    {
        int index = getAttrIndex(attrs, attrNames[i]);
        projAttrIndexes.push_back(index);
        projAttrs.push_back(attrs[index]);
    }

    tmpData = malloc(PAGE_SIZE);
}

RC Project::getNextTuple(void *data)
{
    while(it->getNextTuple(tmpData) != -1) // Full data returned
    {
        readAttrsFromTuple(attrs, projAttrIndexes, tmpData, data); // Parse data
        return 0;
    }
    return -1;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs = projAttrs;
}

Project::~Project()
{
    it = NULL;
    free(tmpData);
}

// BNLJoin
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
{
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    cond = &condition;
    // Get tuple descriptor of two tables
    leftIn->getAttributes(attrs1);
    rightIn->getAttributes(attrs2);
    bufsize1 = getTupleMaxSize(attrs1); // Max size of a tuple
    bufsize2 = getTupleMaxSize(attrs2);
    blockSize = numPages * PAGE_SIZE;
    inputBufferSize = PAGE_SIZE;
    matchedCount = 0;

    inputBuffer = malloc(inputBufferSize);
    block = malloc(blockSize);
    tmpTuple1 = malloc(bufsize1);
    tmpTuple2 = malloc(bufsize2);

    isLastTupleVisited1 = false;
    isLastTupleVisited2 = true;
    isTmpTuple1 = false;
    isTmpTuple2 = false;

    data1 = malloc(bufsize1);
    data2 = malloc(bufsize2);
    index1 = getAttrIndex(attrs1, cond->lhsAttr);
    index2 = getAttrIndex(attrs2, cond->rhsAttr);
    value1 = malloc(attrs1[index1].length + 5); // In case of varchar, 1 byte for null header, 4 bytes for length (int)
    value2 = malloc(attrs2[index2].length + 5);
    //	cout << "bnl.attrs1[id1].type = " << attrs1[index1].type << endl;
}

BNLJoin::~BNLJoin()
{
    leftIn = NULL;
    rightIn = NULL;

    free(inputBuffer);
    free(block);
    free(tmpTuple1);
    free(tmpTuple2);

    free(data1);
    free(data2);
    free(value1);
    free(value2);
}

//template<class T>
RC BNLJoin::loadTuplesIntoBuffer(Iterator *input, bool &isLastTupleVisited, bool &isTmpTuple,
        const vector<Attribute> &attrs, const int &bufferSize, void *tmpTuple,
        void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, bool isOuter)
{
    void *data = malloc(PAGE_SIZE);
    memset(buffer, 0, bufferSize);
    tupleOffsets.clear();
    tupleLengths.clear();
    int offset = 0;
    bool isBlockFull;
    if (isTmpTuple) // The tuple that failed to be loaded into block last time
    {
        isTmpTuple = false;
        int length = getTupleLength(attrs, tmpTuple);
        tupleOffsets.push_back(offset);
        tupleLengths.push_back(length);
        memcpy((char *) buffer + offset, tmpTuple, length);
        isBlockFull = (offset + length + 2 * tupleLengths.size() * sizeof(int) > bufferSize);
        offset += length;
    }

    int rc = input->getNextTuple(data);
    isLastTupleVisited = (rc == -1);
    while (rc != -1)
    {
        int length = getTupleLength(attrs, data);
        isBlockFull = (offset + length + 2 * tupleLengths.size() * sizeof(int) > bufferSize);
        if (!isBlockFull)
        {
            tupleOffsets.push_back(offset);
            tupleLengths.push_back(length);
            memcpy((char *) buffer + offset, data, length);
            offset += length;
            rc = input->getNextTuple(data);
            if (rc == -1)
                isLastTupleVisited = true;
        }
        else // Buffer is full, load this tuple to tmpTuple and go to perform inner scan
        {
            memcpy(tmpTuple, data, length);
            isTmpTuple = true;
            break;
        }
    }
    if (isShowBNL) cout << tupleOffsets.size() << " tuples loaded into buffer\n";

    free(data);
    return 0;
}

template<class T>
void BNLJoin::createHash(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, multimap<T, int> &mm)
{
    // Create hash table for leftIn block
    if (isShowBNL) cout << "**** Creating hash table ****\n";
    mm.clear();
    unsigned i = 0;
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    while (i < tupleOffsets.size()) // Iterator * outer
    {
        memset(data, 0, PAGE_SIZE);
        memset(value, 0, attrs[index].length + 5);
        memcpy(data, (char *)buffer + tupleOffsets[i], tupleLengths[i]);
        readAttrFromTuple(attrs, attrs[index].name, data, value);
        if (*(unsigned char *)value == 0)
        {
            mm.insert(make_pair(*(T *)((char *)value + 1), i));
            // cout << "Inserting <" << *(T *)((char *)value + 1) << ", " << i << ">\n";
        }
        i++;
    }
    free(value);
    free(data);
    if (isShowBNL) cout << "**** End of creating hash table ****\n";
}

void BNLJoin::createHash(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths, multimap<string, int> &mm)
{
    // Create hash table for leftIn block
    if (isShowBNL) cout << "**** Creating hash table ****\n";
    mm.clear();
    unsigned i = 0;
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    while (i < tupleOffsets.size()) // Iterator * outer
    {
        memset(data, 0, PAGE_SIZE);
        memset(value, 0, attrs[index].length + 5);
        memcpy(data, (char *)buffer + tupleOffsets[i], tupleLengths[i]);
        readAttrFromTuple(attrs, attrs[index].name, data, value);
        if (!(*(unsigned char *)value))
        {
            string str((char *)((char *)value + 5));
            mm.insert(make_pair(str, i));
        }
        i++;
    }
    free(value);
    free(data);
    if (isShowBNL)    cout << "**** End of creating hash table ****\n";
}

void BNLJoin::getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
        multimap<int, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2)
{
    if (isShowBNL)
        cout << "\n**** Get matching pairs ****\n";
    matchingIds1.clear();
    matchingIds2.clear();
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    for (unsigned i = 0; i < tupleOffsets.size(); i++)
    {
        memcpy(data, (char *)buffer + tupleOffsets[i], tupleLengths[i]);
        readAttrFromTuple(attrs, attrs[index].name, data, value);
        if (*(unsigned char *)value == 0)
        {
            if (mm.count(*(int *)((char *)value + 1)) > 0)
            {
                //				cout << "Found r.k = " << *(int *)((char *)value + 1) << endl;
                pair<multimap<int, int>::iterator, multimap<int, int>::iterator> range;
                range = mm.equal_range(*(int *)((char *)value + 1));
                for (multimap<int, int>::iterator it = range.first; it != range.second; ++it)
                {
                    //					cout << "Putting " << i << ", ";
                    matchingIds1.push_back(it->second);
                    matchingIds2.push_back(i);
                }
            }
        }
    }
    free(value);
    free(data);
    if (isShowBNL)
        cout << "**** " << matchingIds2.size() << " matching pairs ****\n";
}

void BNLJoin::getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
        multimap<float, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2)
{
    if (isShowBNL)
        cout << "\n**** Get matching pairs ****\n";
    matchingIds1.clear();
    matchingIds2.clear();
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    for (unsigned i = 0; i < tupleOffsets.size(); i++)
    {
        memcpy(data, (char *)buffer + tupleOffsets[i], tupleLengths[i]);
        readAttrFromTuple(attrs, attrs[index].name, data, value);
        if (!(*(unsigned char *)value))
        {
            if (mm.count(*(float *)((char *)value + 1)) > 0)
            {
                pair<multimap<float, int>::iterator, multimap<float, int>::iterator> range;
                range = mm.equal_range(*(float *)((char *)value + 1));
                for (multimap<float, int>::iterator it = range.first; it != range.second; ++it)
                {
                    matchingIds1.push_back(it->second);
                    matchingIds2.push_back(i);
                }
            }
        }
    }
    free(value);
    free(data);
    if (isShowBNL)
        cout << "**** " << matchingIds2.size() << " matching pairs ****\n";
}

void BNLJoin::getMatchingPairs(const vector<Attribute> &attrs, const int index, const void *buffer, vector<int> &tupleOffsets, vector<int> &tupleLengths,
        multimap<string, int> &mm, vector<int> &matchingIds1, vector<int> &matchingIds2)
{
    if (isShowBNL)
        cout << "\n**** Get matching pairs ****\n";
    matchingIds1.clear();
    matchingIds2.clear();
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    for (unsigned i = 0; i < tupleOffsets.size(); i++)
    {
        memset(data, 0, PAGE_SIZE);
        memset(value, 0, attrs[index].length + 5);
        memcpy(data, (char *)buffer + tupleOffsets[i], tupleLengths[i]);
        readAttrFromTuple(attrs, attrs[index].name, data, value);
        string str((char *)((char *)value + 5)); // Ignore null bit and len
        if (mm.count(str) > 0)
        {
            pair<multimap<string, int>::iterator, multimap<string, int>::iterator> range;
            range = mm.equal_range(str);
            for (multimap<string, int>::iterator it = range.first; it != range.second; ++it)
            {
                matchingIds1.push_back(it->second);
                matchingIds2.push_back(i);
            }
        }
    }
    free(value);
    free(data);
    if (isShowBNL)
        cout << "**** " << matchingIds2.size() << " matching pairs ****\n";
}

RC BNLJoin::getNextTuple(void *data)
{
    //	if (!cond->bRhsIsAttr)
    //		return -1;
    bool allPairsScanned = matchedCount == matchingIds1.size();
    bool isLoadBlockEnd = isLastTupleVisited1 && isLastTupleVisited2 && allPairsScanned;

    while (!isLoadBlockEnd)
    {
        if (!isLastTupleVisited1 && isLastTupleVisited2 && allPairsScanned) // load tuples info block
        {
            isLastTupleVisited2 = false;
            loadTuplesIntoBuffer(leftIn, isLastTupleVisited1, isTmpTuple1, attrs1, blockSize, tmpTuple1, block, tupleOffsets1, tupleLengths1, true);
            // Reset inner TableScan
            rightIn->setIterator();
            // Create hash
            if (attrs1[index1].type == TypeInt)
                createHash<int>(attrs1, index1, block, tupleOffsets1, tupleLengths1, mm_int);
            else if (attrs1[index1].type == TypeReal)
                createHash<float>(attrs1, index1, block, tupleOffsets1, tupleLengths1, mm_float);
            else
                createHash(attrs1, index1, block, tupleOffsets1, tupleLengths1, mm_str);
        }
        if (allPairsScanned)
        {
            if (isShowBNL)
                cout << "Load page...";
            matchedCount = 0;
            // Load tuples into a page
            loadTuplesIntoBuffer(rightIn, isLastTupleVisited2, isTmpTuple2, attrs2, inputBufferSize, tmpTuple2, inputBuffer, tupleOffsets2, tupleLengths2, false);

            // Get matching pairs by scanning a inner page
            if (attrs2[index2].type == TypeInt)
                getMatchingPairs(attrs2, index2, inputBuffer, tupleOffsets2, tupleLengths2, mm_int, matchingIds1, matchingIds2);
            else if (attrs2[index2].type == TypeReal)
                getMatchingPairs(attrs2, index2, inputBuffer, tupleOffsets2, tupleLengths2, mm_float, matchingIds1, matchingIds2);
            else
                getMatchingPairs(attrs2, index2, inputBuffer, tupleOffsets2, tupleLengths2, mm_str, matchingIds1, matchingIds2);
        }

        // Fetch matching pairs
        if (matchedCount < matchingIds1.size())
        {
            memcpy(data1, (char *)block + tupleOffsets1[matchingIds1[matchedCount]], tupleLengths1[matchingIds1[matchedCount]]);
            memcpy(data2, (char *)inputBuffer + tupleOffsets2[matchingIds2[matchedCount]], tupleLengths2[matchingIds2[matchedCount]]);
            joinTuple(attrs1, attrs2, data1, data2, data); // process data1 and data2
            matchedCount ++;
            return 0;
        }
        allPairsScanned = matchedCount == matchingIds1.size();
        isLoadBlockEnd = isLastTupleVisited1 && isLastTupleVisited2 && allPairsScanned;
    }
    if (isShowBNL)
        cout << "Last blocked visited\n";
    return -1;  // End of BNLJoin
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear(); // Clear attrs and join attrs1 and attrs2
    for (unsigned i = 0; i < attrs1.size(); i++)
        attrs.push_back(attrs1[i]);
    for (unsigned i = 0; i < attrs2.size(); i++)
        attrs.push_back(attrs2[i]);
}

// INLJoin

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    cond = &condition;
    // Get tuple descriptor of two tables
    leftIn->getAttributes(attrs1);
    rightIn->getAttributes(attrs2);
    bufsize1 = getTupleMaxSize(attrs1); // Max size of a tuple
    bufsize2 = getTupleMaxSize(attrs2);
    data1 = malloc(bufsize1);
    data2 = malloc(bufsize2);
    index1 = getAttrIndex(attrs1, cond->lhsAttr);
    index2 = getAttrIndex(attrs2, cond->rhsAttr);
    value1 = malloc(attrs1[index1].length + 5); // In case of varchar, 1 byte for null header, 4 bytes for length (int)
    value2 = malloc(attrs2[index2].length + 5);
}

INLJoin::~INLJoin()
{
    leftIn = NULL;
    rightIn = NULL;
    free(data1);
    free(data2);
    free(value1);
    free(value2);
}

RC INLJoin::getNextTuple(void *data)
{
    if (!cond->bRhsIsAttr)
        return -1;
    while (leftIn->getNextTuple(data1) != -1) // Iterator * outer
    {
        readAttrFromTuple(attrs1, cond->lhsAttr, data1, value1);
        rightIn->setIterator(((char *)value1 + 1), ((char *)value1 + 1), true, true);
        while (rightIn->getNextTuple(data2) != -1) // TableScan * inner
        {
            //			cout << *(float *)((char *)value1 + 1) << endl;
            joinTuple(attrs1, attrs2, data1, data2, data); // process data1 and data2
            return 0;
        }
        return -1;
        //		while (rightIn->getNextTuple(data2) != -1) // TableScan * inner
        //		{
        //			readAttrFromTuple(attrs2, cond->rhsAttr, data2, value2);
        //			// If match (left.A == right.B), then concatenate them to generate returned data
        //			if (isMatch(attrs1[index1].type, value1, value2, cond->op))
        //			{
        //				joinTuple(attrs1, attrs2, data1, data2, data); // process data1 and data2
        //				return 0;
        //			}
        //		}
    }
    return -1;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear(); // Clear attrs and join attrs1 and attrs2
    for (unsigned i = 0; i < attrs1.size(); i++)
        attrs.push_back(attrs1[i]);
    for (unsigned i = 0; i < attrs2.size(); i++)
        attrs.push_back(attrs2[i]);
}

// Aggregate default constructor
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
    it = input;
    this->aggAttr = aggAttr;
    this->op = op;
    it->getAttributes(attrs);
    index = getAttrIndex(attrs, aggAttr.name);
    bufsize = getTupleMaxSize(attrs); // Max size of a tuple
    tmpData = malloc(bufsize);

    attrVal = malloc(attrs[index].length + 5);

    tupleCount = 0;
    sum = 0.0;
    opCount = 0;
    hasGroupBy = false;
}

RC Aggregate::groupByAttr(Iterator *input, Attribute groupAttr, const vector<Attribute> attrs, int index, int &numGroups)
{
    cout << "**** GROUP BY attribute(" << attrs[index].name << ") ****\n";
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attrs[index].length + 5);
    RID rid;
    string groupName;
    if (attrs[index].type == TypeInt)
    {
        while (input->getNextTuple(data) != -1)
        {
            readAttrFromTuple(attrs, attrs[index].name, data, value); // Parse data
            if (*(unsigned char *)value == 0) // Attribute value not null
            {
                map<int, int>::iterator it = map_int.find(*(int *)((char *)value + 1));
                if (it != map_int.end()) // Group found!
                    groupName = "tmp_group_" + to_string(it->second);
                else // Such group does not exist, then create it
                {
                    numGroups ++;
                    map_int.insert(pair<int, int>(*(int *)((char *)value + 1), numGroups));
                    groupName = "tmp_group_" + to_string(numGroups);
                    rm->createTable(groupName, attrs);
                    cout << "**** Create group(" << groupName << ") ****\n";
                    //					cout << "Inserting <" << *(int *)((char *)value + 1) << ", " << numGroups << ">\n";
                }
                rm->insertTuple(groupName, data, rid);
                //				cout << "Inserting data.key = " << *(int *)((char *)value + 1) << " to " << groupName << "\n";
            }
        }
    }
    else if (attrs[index].type == TypeReal)
    {
        while (input->getNextTuple(data) != -1)
        {
            readAttrFromTuple(attrs, attrs[index].name, data, value); // Parse data
            if (*(unsigned char *)value == 0) // Attribute value not null
            {
                map<float, int>::iterator it = map_float.find(*(float *)((char *)value + 1));
                if (it != map_float.end()) // Group found!
                    groupName = "tmp_group_" + to_string(it->second);
                else // Such group does not exist, then create it
                {
                    numGroups ++;
                    map_float.insert(pair<float, int>(*(float *)((char *)value + 1), numGroups));
                    groupName = "tmp_group_" + to_string(numGroups);
                    rm->createTable(groupName, attrs);
                    cout << "**** Create group(" << groupName << ") ****\n";
                }
                rm->insertTuple(groupName, data, rid);
            }
        }
    }
    else
    {
        while (input->getNextTuple(data) != -1)
        {
            memset(value, 0, attrs[index].length + 5);
            readAttrFromTuple(attrs, attrs[index].name, data, value); // Parse data
            if (*(unsigned char *)value == 0) // Attribute value not null
            {
                string str((char *)value + 5);
                map<string, int>::iterator it = map_str.find(str);
                if (it != map_str.end()) // Group found!
                    groupName = "tmp_group_" + to_string(it->second);
                else // Such group does not exist, then create it
                {
                    numGroups ++;
                    map_str.insert(pair<string, int>(str, numGroups));
                    groupName = "tmp_group_" + to_string(numGroups);
                    rm->createTable(groupName, attrs);
                    cout << "**** Create group(" << groupName << ") ****\n";
                }
                rm->insertTuple(groupName, data, rid);
            }
        }
    }
    //	if (numGroups != (int)map_int.size())
    //		cout << "Number of groups is incorrect\n";
    //	else
    cout << "**** " << numGroups << " groups created based on (" << attrs[index].name << ") ****\n";
    free(value);
    free(data);
    return 0;
}

// Aggregate constructor with group by attribute
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op)
{
    it = input;
    this->aggAttr = aggAttr;
    this->gAttr = groupAttr;
    this->op = op;
    it->getAttributes(attrs);
    index = getAttrIndex(attrs, aggAttr.name);
    bufsize = getTupleMaxSize(attrs); // Max size of a tuple
    tmpData = malloc(bufsize);

    attrVal = malloc(attrs[index].length + 5);

    tupleCount = 0;
    sum = 0.0;
    opCount = 0;

    rm = RelationManager::instance();
    hasGroupBy = true;
    numGroups = 0; // total number of groups
    curGroupId = 1; // current group id [1, numGroups]
    gIndex = getAttrIndex(attrs, groupAttr.name);
    aggAttr_g.type = aggAttr.type;
    aggAttr_g.length = aggAttr.length;

    // Perform group-based hash aggregation
    groupByAttr(input, groupAttr, attrs, gIndex, numGroups);
}

Aggregate::~Aggregate()
{
    it = NULL;
    free(tmpData);
    free(attrVal);

    // Clean temperate group files
    if (hasGroupBy)
    {
        for (int i = 1; i <= numGroups; i++)
        {
            string groupName = "tmp_group_" + to_string(i);
            rm->deleteTable(groupName);
            cout << "**** Clean group(" << groupName << ") ****\n";
        }
    }
}

// Return a tuple, used in getNextTuple
RC Aggregate::getTuple(void *data)
{
    // Cannot perform MIN, MAX, SUM, AVG for varchar type
    if (opCount == 1 || (op != COUNT && aggAttr.type == TypeVarChar) || it->getNextTuple(tmpData) == -1)
    {
        opCount = 0; // Reset to 0, so that aggregate->getNextTuple() can be used properly next time
        *(unsigned char *) data = (1 << 7);
        return -1;
    }
    // Initialize (returned) void *data
    readAttrFromTuple(attrs, aggAttr.name, tmpData, data); // Parse data
    if (*(unsigned char *)data == 0)
    {
        tupleCount ++;
        if (aggAttr.type == TypeInt)
            sum += *(int *)((char *)data + 1);
        else if (aggAttr.type == TypeReal)
            sum += *(float *)((char *)data + 1);
    }

    while(it->getNextTuple(tmpData) != -1) // Full tuple data returned
    {
        readAttrFromTuple(attrs, aggAttr.name, tmpData, attrVal); // Parse data
        if (*(unsigned char *)attrVal == 0) // If nullBit = 1, continue
        {
            tupleCount ++;
            if (op == MIN)
            {
                if (isMatch(attrs[index].type, attrVal, data, LT_OP))
                    memcpy((char *)data + 1, (char *)attrVal + 1, 1 + aggAttr.length);
            }
            else if (op == MAX)
            {
                if (isMatch(attrs[index].type, attrVal, data, GT_OP))
                    memcpy((char *)data + 1, (char *)attrVal + 1, 1 + aggAttr.length);
            }
            if (aggAttr.type == TypeInt)
                sum += *(int *)((char *)attrVal + 1);
            else if (aggAttr.type == TypeReal)
                sum += *(float *)((char *)attrVal + 1);
        }
    }
    // An aggregation returns a float value
    *(unsigned char *) data = 0;
    if (op == COUNT)
        *(float *)((char *)data + 1) = (float) tupleCount;
    else if (op == SUM)
    {
        *(float *)((char *)data + 1) = sum;
    }
    else if (op == AVG)
    {
        avg = sum / tupleCount;
        *(float *)((char *)data + 1) = avg;
    }
    else // MAX, MIN
    {
        if (aggAttr.type == TypeInt)
            *(float *)((char *)data + 1) = (float) (*(int *)((char *)data + 1));
    }
    opCount ++;
    return 0;
}

RC Aggregate::getNextTuple(void *data)
{
    if (!hasGroupBy)
    {
        return getTuple(data);
    }
    else
    {
        if (curGroupId <= numGroups)
        {
            string groupName = "tmp_group_" + to_string(curGroupId); // name of group file
            cout << "\n**** In Group (" << groupName << ") ****\n";
            aggAttr_g.name = groupName + "." + aggAttr.name;
            TableScan *input = new TableScan(*rm, groupName);
            Aggregate *agg = new Aggregate(input, aggAttr_g, op);
            int t = agg->getTuple(data);
            delete input;
            delete agg;
            if (t != -1)
            {
                // Join groupby value and agg value
                if (aggAttr.type == TypeVarChar) // Check group.A
                {
                    memmove((char *)data + 1 + sizeof(float), (char *)data + 1, sizeof(float));
                    for (map<string, int>::iterator it = map_str.begin(); it != map_str.end(); ++it)
                    {
                        if (it->second == curGroupId)
                        {
                            memcpy((char *)data + 1, (it->first).c_str(), (it->first).length() + 4); // Insert Group.B
                            break;
                        }
                    }
                }
                else
                {
                    memmove((char *)data + 1 + 4, (char *)data + 1, sizeof(float));
                    //					cout << "(In) group.A = " << *(float *)((char *)data + 1) << ", ";
                    if (aggAttr.type == TypeInt) // Int key
                    {
                        for (map<int, int>::iterator it = map_int.begin(); it != map_int.end(); ++it)
                        {
                            if (it->second == curGroupId)
                            {
                                *(int *)((char *)data + 1) = it->first;
                                //								cout << "group.B = " << it->first << endl;
                                break;
                            }
                        }
                    }
                    else // Float key
                    {
                        for (map<float, int>::iterator it = map_float.begin(); it != map_float.end(); ++it)
                        {
                            if (it->second == curGroupId)
                            {
                                *(float *)((char *)data + 1) = it->first;
                                break;
                            }
                        }
                    }
                }
                curGroupId ++;
                return 0;
            }
        }
        return -1;
    }

}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    it->getAttributes(attrs);
}

// Write tuples into partitions
RC writePartition(Iterator *input, const unsigned numPart, const string namePrefix, const vector<Attribute> &attrs, const Attribute &attr)//, unsigned &totalPages)
{
    cout << "**** Creating partitions of " << namePrefix << " ****\n";
    RelationManager *rm2 = RelationManager::instance();
    FileHandle fileHandle;
    // Create "numPart" partition files, eg: left_join1_part
    for (unsigned i = 1; i <= numPart; i++)
    {
        string partName = namePrefix + "_join" + to_string(i) + "_part"; // name of partition file
        //		rbf->destroyFile(partName);
        //		if (rbf->createFile(partName) == -1)
        //			return -1;
        rm2->deleteTable(partName);
        if (rm2->createTable(partName, attrs) == -1)
            return -1;
        //		cout << "partition(" << partName << ")\n";
    }
    void *data = malloc(PAGE_SIZE);
    void *value = malloc(attr.length + 5);
    RID rid;
    unsigned partId; // Id allocated for partition, 0~9

    if (attr.type == TypeInt)
    {
        while(input->getNextTuple(data) != -1)
        {
            memset(data, 0, PAGE_SIZE);
            memset(value, 0, attr.length + 5);
            readAttrFromTuple(attrs, attr.name, data, value);
            if (!(*(unsigned char *)value))
            {
                partId = *(int *)((char *)value + 1) % numPart; // 0~9
                string partName = namePrefix + "_join" + to_string(partId + 1) + "_part"; // name of partition file
                rm2->insertTuple(partName, data, rid);
                //				rbf->openFile(partName, fileHandle);
                //				rbf->insertRecord(fileHandle, attrs, data, rid);
                //				rbf->closeFile(fileHandle);
            }
        }
    }
    else if (attr.type == TypeReal)
    {
        memset(data, 0, PAGE_SIZE);
        while(input->getNextTuple(data) != -1)
        {
            memset(value, 0, attr.length + 5);
            readAttrFromTuple(attrs, attr.name, data, value);
            if (!(*(unsigned char *)value))
            {
                partId = int(*(float *)((char *)value + 1)) % numPart;
                if (!isShowGH && *(float *)((char *)value + 1) >= 50)
                    cout << *(float *)((char *)value + 1) << " goes to p[" << partId << "]\n";
                string partName = namePrefix + "_join" + to_string(partId + 1) + "_part"; // name of partition file
                rm2->insertTuple(partName, data, rid);
                //				rbf->openFile(partName, fileHandle);
                //				rbf->insertRecord(fileHandle, attrs, data, rid);
                //				rbf->closeFile(fileHandle);
            }
            else
                cout << "NULL attr.val\n";
        }
    }
    else
    {
        while(input->getNextTuple(data) != -1)
        {
            memset(value, 0, attr.length + 5);
            readAttrFromTuple(attrs, attr.name, data, value);
            if (!(*(unsigned char *)value))
            {
                partId = int(*(float *)((char *)value + 1)) % numPart;
                string partName = namePrefix + "_join" + to_string(partId + 1) + "_part"; // name of partition file
                rm2->insertTuple(partName, data, rid);
                //				rbf->openFile(partName, fileHandle);
                //				rbf->insertRecord(fileHandle, attrs, data, rid);
                //				rbf->closeFile(fileHandle);
            }
        }
    }

    rm2 = NULL;
    // Scan all partitions to get number of pages
    RecordBasedFileManager *rbf = RecordBasedFileManager::instance();
    unsigned totalPages = 0; // Calculate number of pages
    for (unsigned i = 1; i <= numPart; i++)
    {
        string partName = namePrefix + "_join" + to_string(i) + "_part"; // name of partition file
        rbf->openFile(partName, fileHandle);
        unsigned numPages = fileHandle.getNumberOfPages();
        totalPages += numPages;
        int tupleSum = 0;
        for (unsigned j = 0; j < numPages; j++)
        {
            fileHandle.readPage(j, data);
            unsigned short cnt = *(unsigned short *)((char *)data + PAGE_SIZE - 2*2);
            tupleSum += cnt;
        }
        if (isShowGH)
            cout << namePrefix << " partition(" << (i) << ") has " << numPages << " pages, " << tupleSum << " tuples\n";
        rbf->closeFile(fileHandle);
    }
    rbf = NULL;
    cout << "**** End creating partitions of " << namePrefix << " ****\n";
    free(value);
    free(data);
    return 0;
}

// Grace hash join
GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions)
{
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->cond = &condition;
    this->numPart = numPartitions;

    lastPartId = -1;
    curPartId = 1;
    cond2.op = EQ_OP;
    cond2.bRhsIsAttr = true;

    rm = RelationManager::instance();

    // Get tuple descriptor of two tables
    leftIn->getAttributes(attrs1);
    rightIn->getAttributes(attrs2);
    index1 = getAttrIndex(attrs1, cond->lhsAttr);
    index2 = getAttrIndex(attrs2, cond->rhsAttr);
    //	unsigned totalPages1;
    //	unsigned totalPages2;

    // Partition R and S, write tuples into partitions
    int rc = writePartition(leftIn, numPartitions, "left", attrs1, attrs1[index1]);//, totalPages1);
    if (rc == -1)
        cout << "write to partitions failed\n";
    rc = writePartition(rightIn, numPartitions, "right", attrs2, attrs2[index2]);//, totalPages2);
}

GHJoin::~GHJoin()
{
    //	cout << "**** Begin ~GHJoin() ****\n";
    leftIn = NULL;
    rightIn = NULL;

    // Destroy partition files
    for (unsigned i = 1; i <= numPart; i++)
    {
        string partName = "left_join" + to_string(i) + "_part"; // name of partition file
        rm->deleteTable(partName);
        partName = "right_join" + to_string(i) + "_part"; // name of partition file
        rm->deleteTable(partName);
    }
    rm = NULL;
    pleftIn = NULL;
    prightIn = NULL;
    bnlJoin = NULL;
    //	cout << "**** End ~GHJoin() ****\n";
}

RC GHJoin::getNextTuple(void *data)
{
    //	return -1;
    //	if (cond->op != EQ_OP) // Grace hash join can only handle EQ_OP
    //		return -1;
    while (curPartId <= numPart)
    {
        if (curPartId != lastPartId)
        {
            // Name of attr has changed to left_join1_part.largeleft.A!!!! So a new condition is needed!
            lastPartId = curPartId;
            string partName = "left_join" + to_string(curPartId) + "_part"; // name of partition file
            cout << "\n**** Scanning " << partName << " ****\n";
            cond2.lhsAttr = partName + "." + cond->lhsAttr;
            pleftIn = new TableScan(*rm, partName);
            partName = "right_join" + to_string(curPartId) + "_part"; // name of partition file
            prightIn = new TableScan(*rm, partName);
            cond2.rhsAttr = partName + "." + cond->rhsAttr;
            bnlJoin = new BNLJoin(pleftIn, prightIn, cond2, numPart);
        }
        int t = bnlJoin->getNextTuple(data);
        if (t != -1)
        {
            //			if (isDebugGH)
            //			{
            //				vector<Attribute> attrs;
            //				getAttributes(attrs);
            //				rm->printTuple(attrs, data);
            //			}
            return 0;
        }
        delete pleftIn;
        delete prightIn;
        delete bnlJoin;
        curPartId ++; // Continue to scan next
    }
    if (isShowGH)
        cout << "**** Last partition scanned ****\n";
    return -1;  // End of GHJoin
}


void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear(); // Clear attrs and join attrs1 and attrs2
    attrs.insert(attrs.end(), attrs1.begin(), attrs1.end());
    attrs.insert(attrs.end(), attrs2.begin(), attrs2.end());
}
