
#include "rm.h"

#include <algorithm>
#include <cstring>
#include <iostream>
using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(INDEXES_TABLE_ID, 1, INDEXES_TABLE_NAME);
    if (rc)
        return rc;

    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(INDEXES_TABLE_ID, indexDescriptor);
    if (rc)
        return rc;

    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

// TODO:
// 1) Delete index catalog rows for this table id and their associated btree files
RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) 
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    if (rc)
        return rc;

    rc = updateIndexes(tableName, recordDescriptor, data, rid, INDEX_INSERT);
    if (rc)
        return rc;

    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    void *oldData = malloc(PAGE_SIZE);

    // Read old record
    rc = readTuple(tableName, rid, oldData);
    if (rc)
        return rc;

    // Delete old record key
    rc = updateIndexes(tableName, recordDescriptor, oldData, rid, INDEX_DELETE);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    void *oldData = malloc(PAGE_SIZE);

    // Read old record
    rc = readTuple(tableName, rid, oldData);
    if (rc)
        return rc;

    // Delete old record key
    rc = updateIndexes(tableName, recordDescriptor, data, rid, INDEX_DELETE);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    // and then insert new key for new record
    rc = updateIndexes(tableName, recordDescriptor, data, rid, INDEX_INSERT);
    if (rc)
        return rc;

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

string RelationManager::indexFileName(const string &tableName, const char *indexName)
{
    return tableName + "_" + string(indexName) + string(INDEX_FILE_EXTENSION);
}

string RelationManager::indexFileName(const string &tableName, const string &indexName)
{
    return tableName + "_" + indexName + string(INDEX_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
    vector<Attribute> id;

    Attribute attr;
    attr.name = INDEXES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    id.push_back(attr);

    attr.name = INDEXES_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_COLUMN_NAME_SIZE;
    id.push_back(attr);

    return id;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

void RelationManager::prepareIndexesRecordData(int32_t tid, const string &attributeName, void *data)
{
    unsigned offset = 0;
    int32_t attr_len = attributeName.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &tid, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &attr_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attributeName.c_str(), attr_len);
    offset += attr_len;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

RC RelationManager::insertIndex(int32_t tid, const string &attributeName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *indexData = malloc (INDEXES_RECORD_DATA_SIZE);
    prepareIndexesRecordData(tid, attributeName, indexData);
    rc = rbfm->insertRecord(fileHandle, indexDescriptor, indexData, rid);

    rbfm->closeFile(fileHandle);
    free (indexData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;   
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);
    
    real = tmp;
}

// RC RelationManager::scanDescriptorForIndex(const string &tableName, const vector<Attribute> recordDescriptor, const void* data, const RID &rid) 
// {
//     RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

//     char *start = (char*) data;

//     // Allocate space for null indicator. The returned null indicator may be larger than
//     // the null indicator in the table has had fields added to it
//     int nullIndicatorSize = rbfm->getNullIndicatorSize(recordDescriptor.size());
//     char nullIndicator[nullIndicatorSize];
//     memset(nullIndicator, 0, nullIndicatorSize);

//     // Get number of columns and size of the null indicator for this record
//     RecordLength len = 0;
//     memcpy (&len, (char*)page + offset, sizeof(RecordLength));
//     int recordNullIndicatorSize =rbfm-> getNullIndicatorSize(len);

//     // Read in the existing null indicator
//     memcpy (nullIndicator, start + sizeof(RecordLength), nullIndicatorSize);

//     // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
//     for (unsigned i = len; i < recordDescriptor.size(); i++)
//     {
//         int indicatorIndex = (i+1) / CHAR_BIT;
//         int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
//         nullIndicator[indicatorIndex] |= indicatorMask;
//     }
//     // Write out null indicator
//     memcpy(data, nullIndicator, nullIndicatorSize);

//     // Initialize some offsets
//     // rec_offset: points to data in the record. We move this forward as we read data from our record
//     unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
//     // data_offset: points to our current place in the output data. We move this forward as we write data to data.
//     unsigned data_offset = nullIndicatorSize;
//     // directory_base: points to the start of our directory of indices
//     char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
//     for (unsigned i = 0; i < recordDescriptor.size(); i++)
//     {
//         if (fieldIsNull(nullIndicator, i))
//             continue;
        
//         // Grab pointer to end of this column
//         ColumnOffset endPointer;
//         memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

//         // rec_offset keeps track of start of column, so end-start = total size
//         uint32_t fieldSize = endPointer - rec_offset;

//         // Special case for varchar, we must give data the size of varchar first
//         if (recordDescriptor[i].type == TypeVarChar)
//         {
//             memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
//             data_offset += VARCHAR_LENGTH_SIZE;
//         }
//         // Next we copy bytes equal to the size of the field and increase our offsets
//         memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
//         rec_offset += fieldSize;
//         data_offset += fieldSize;
//     }
// }

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

// TODO:
// 1) Make sure table isn't system and exists
// 2) Make sure attribute exists on table
// 3) Make sure index catalog doesnt have this (table, col) already
// 4) insertIndex() and IX::createIndex() loop to make b-tree index file
RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    bool isSystem;
    RC rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    bool colExists = false;
    int colPos = -1;
    for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
        if(recordDescriptor[i].name == attributeName)  {
            colExists = true;  
            colPos = i;
            break;
        }
    }

    if(!colExists)
        return -1;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(INDEXES_COL_COLUMN_NAME);

    void *value = &id;
    rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc(INDEXES_RECORD_DATA_SIZE);
    bool rowExists = false;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        unsigned offset = 1;

        int32_t colLen;
        memcpy(&colLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;

        char col[colLen + 1];
        col[colLen] = '\0';
        memcpy(col, (char*) data + offset, colLen);

        if(strcmp(col, attributeName.c_str()) == 0) {
            rowExists = true; break;
        }
    }

    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);

    if(rowExists)
        return -1;

    if (rc != RBFM_EOF)
        return rc;

    rc = insertIndex(id, attributeName);
    if (rc)
        return rc;

    IXFileHandle ixfileHandle;
    IndexManager *im = IndexManager::instance();

    rc = im->createFile(indexFileName(tableName, attributeName));
    if (rc)
        return rc;

    rc = im->openFile(indexFileName(tableName, attributeName), ixfileHandle);
    if (rc)
        return rc;

    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    projection.clear();
    projection.push_back(attributeName);

    value = &id;
    rc = rbfm->scan(fileHandle, recordDescriptor, attributeName, NO_OP, NULL, projection, rbfm_si);

    data = malloc(1 + recordDescriptor[colPos].length);
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {

        char null;
        memcpy(&null, data, 1);
        if (null)
            continue;

        rc = im->insertEntry(ixfileHandle, recordDescriptor[colPos], (char*) data + 1, rid);
        if (rc)
            return -1;
    }

    if (rc != RBFM_EOF)
        return rc;

    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    im->closeFile(ixfileHandle);
    free(data);

	return SUCCESS;
}

// TODO:
// 1) Delete tuples from indexes table 
// 2) Delete file index from IX
RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    RC rc = 0;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(INDEXES_COL_COLUMN_NAME);

    void *value = &id;
    rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    IndexManager *im = IndexManager::instance();
    rc = im->destroyFile(indexFileName(tableName, attributeName));
    if (rc)
        return rc;

	return SUCCESS;
}

RC RelationManager::getValue(const string name, const vector<Attribute> &attrs, const void* data, void* value) 
{
    int offset = ceil(attrs.size() / 8.0);
    for (size_t i = 0; i < attrs.size(); ++i) {
        char target = *((char*)data + i/8);
        if (target & (1<<(7-i%8))) {
            if (name == attrs[i].name) {
                return -1;
            }
            else  {
                continue;
            }
        }
        int size = sizeof(int);
        if (attrs[i].type == TypeVarChar) {
            memcpy(&size, (char*)data + offset, sizeof(int));
            memcpy((char*)value, &size, sizeof(int));
            memcpy((char*)value + sizeof(int), (char*)data + offset + sizeof(int), size);
            size += sizeof(int);
        } else 
            memcpy(value, (char*)data + offset, sizeof(int));                  
        if (name == attrs[i].name)
            return size;       
        offset += size;
    }
    return 0;
}

RC RelationManager::updateIndexes(const string& tableName, const vector<Attribute> recordDescriptor, const void* data, const RID& rid, char flag) {

    FileHandle fileHandle;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(INDEXES_COL_COLUMN_NAME);

    int32_t id = INDEXES_TABLE_ID;
    void *value = &id;
    rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID recordRID;
    void *recordData = malloc(1 + INT_SIZE + INDEXES_COL_COLUMN_NAME_SIZE);
    vector<IndexTuple> indexes;
    while ((rc = rbfm_si.getNextRecord(recordRID, recordData)) == SUCCESS)
    {
        unsigned offset = 1;

        int32_t colLen;
        memcpy(&colLen, (char*) recordData + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;

        char col[colLen + 1];
        col[colLen] = '\0';
        memcpy(col, (char*) recordData + offset, colLen);

        indexes.push_back(make_tuple(string {col}, -1));
    }

    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(recordData);

    // assuming order is insert order
    for(unsigned i = 0; i < recordDescriptor.size(); ++i) {
        if(i >= indexes.size()) break;
        if(recordDescriptor[i].name == get<TupleColumn>(indexes[i])) {
            indexes[i] = make_tuple(get<TupleColumn>(indexes[i]), i);
        }
    }

    for(auto& index:  indexes) {
        rc = getValue(get<TupleColumn>(index), recordDescriptor, data, value); 
        if(rc <= 0) {
            continue;
        }

        IXFileHandle ixfileHandle;
        IndexManager *im = IndexManager::instance();

        rc = im->openFile(indexFileName(tableName, get<TupleColumn>(index)), ixfileHandle);
        if (rc)
            return rc;

        if(flag == INDEX_DELETE)
            rc = im->deleteEntry(ixfileHandle, recordDescriptor[get<TupleIndex>(index)], value, rid);
        if(flag == INDEX_INSERT)
            rc = im->insertEntry(ixfileHandle, recordDescriptor[get<TupleIndex>(index)], value, rid);

        if (rc)
            return rc;

        im->closeFile(ixfileHandle);
    }

    return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
    rm_IndexScanIterator.ix_iter = *(new IX_ScanIterator());

    vector<Attribute> recordDescriptor;
    RC rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    bool colExists = false;
    int colPos = -1;
    for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
        if(recordDescriptor[i].name == attributeName)  {
            colExists = true;  
            colPos = i;
            break;
        }
    }

    if(!colExists)
        return -1;

    IndexManager *im = IndexManager::instance();
    rm_IndexScanIterator.ix_iter.fileHandle = new IXFileHandle();
    rc = im->openFile(indexFileName(tableName, attributeName), *rm_IndexScanIterator.ix_iter.fileHandle);
    if (rc)
        return rc;

    rc = rm_IndexScanIterator.ix_iter.initialize(*rm_IndexScanIterator.ix_iter.fileHandle, recordDescriptor[colPos], lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    if(rc)
        return rc;

	return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
    return ix_iter.getNextEntry(rid, key);
}  


RC RM_IndexScanIterator::close() {
    IndexManager *im = IndexManager::instance();

    RC rc = ix_iter.close();
    if (rc)
        return rc;

    rc = im->closeFile(*ix_iter.fileHandle);
    if (rc)
        return rc;

    return SUCCESS;
}




    // // 1) Make sure table exists
    // bool tableExists;
    // int tableId;

    // RC rc = tableExists(tableExists, tableName, tableId);
    // if (rc)
    //     return rc;

    // if(!tableExists)
    //     return -1;

    // // 2) Make sure attribute exists
    // bool columnExists;
    // rc = columnExists(columnExists, tableId, attributeName);
    // if (rc)
    //     return rc;

    // if(!columnExists)
    //     return -1;
