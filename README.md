# cs222-project
The current directory contains project descriptions, source code, test cases, test scripts and output logs.
### To test a project
```sh
./run-test.sh
```
check run-test.sh for more details

### lines of source code (not including test code)  in each project (total: ~8k)
* project 1 (record based file manager): 1.3k
* project 2 (relation manager): 1.0k
* project 3 (index manager): 2.9k
* project 4 (query engine): 1.5k

## summary of 4 projects
4/22/2017

### project 1
PagedFileManager (PFM):
- createFile, destroyFile, openFile, closeFile

FileHandler:
- readPage, writePage, appendPage, getNumberOfPages, collectCounterValues

RecordBasedFileManager (RBFM):
- create/destroy/open/close file (use a private member: PagedFileManager *pfm = PagedFileManager::instance();)
- CRUD operations of record: insertRecord, readRecord, updateRecord, deleteRecord
  - update record has 3 cases: 
    - new record is smaller than original, then shift following records backward if any
    - new record is greater than original, but can still be fitted in that page, then shift its following records backward if any
    - new record is greater than original, cannot be fitted in that page, then check all other pages for space, if no luck, append a new page; leave a mark in the original record, indicating the record is updated and moved to other place, refer the new page number and slot number for it (not the same indicators as deleted record
  - delete record
    - if target record can be found, remove it, shift following record backward, update slot directory
    - if target record is deleted already, return failure code (re-deletion should fail)
    - if it is moved to somewhere else, fetch the new page number and slot number, delete it recursively
- readAttribute(attrName)
#### tips
use inline functions to increase code flexibility, e.g. 
- use getter/setter functions for record offset/length, free space pointer, slot counter, etc. instead of direct pointer access (with offset), when the types of such variables change (from short to int) or a new design is used to get O(1) access of an attribute using field offsets, it will not bring much modification; 
- get nulls indicator header size in bytes given number of fields.
when a new function is introduced, unit test it outside and validate input with assertions before it introduces potential bugs to existing code.
Always verify file operations are valid, no reopen, recreate or unclose error, open/create/close/remove should all succeed normally.

### project 2
Relation Manager (RM):
- create/deleteCatalog, create/deleteTable
- getAttributes(tableName), need to scan catalog tables
- insert/read/delete/update tuple (same as RBFM::CRUD)
- scan (returns an iterator to allow the caller to call getNext())

### project 3
Index Manager (IX)
- create/delete/update index file
- insert/delete index entry (B+ tree)
- index scan (all operators, getNext())

### project 4
Query Engine (QE)
- relation manager (index file scan/create/delete)
- implement the following query types (iterator interface)
  - filter (selected rows)
  - project (selected columns) 
  - join (block nested-loop join, index nested-loop join, grace hash join)
  - aggregate (min, max, sum, avg, count)

