
#include "rm.h"

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    int rc = rbfmScanIterator->getNextRecord(rid, data);
    if(rc != 0) return RM_EOF;

    return 0;
}

RM_ScanIterator::RM_ScanIterator() {
    
}

RM_ScanIterator::~RM_ScanIterator() {
}

RC RM_ScanIterator::close() {   
   RecordBasedFileManager::instance()->closeFile(*fileHandle);
   delete fileHandle;
   rbfmScanIterator->close();
   delete rbfmScanIterator;

   return 0;
}

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
   _rbfm = RecordBasedFileManager::instance();
   FILE* exists = fopen("tables.stat", "r");
   if(exists == NULL) {
        stats = fopen("tables.stat", "wrb+");
   } else {
       fclose(exists);
       stats = fopen("tables.stat", "rb+");
   }
   assert(stats != NULL);
}

RelationManager::~RelationManager()
{
   _rbfm->DestroyInstance();
   _rbfm = NULL;
   fflush(stats);
   fclose(stats);
}

RC RelationManager::createCatalog()
{
    int tbl_rc = _rbfm->createFile("Tables.tbl");
    int col_rc = _rbfm->createFile("Columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    tbl_rc = insertTableRecords();
    col_rc = insertColumnRecords();
    if(tbl_rc != 0 || col_rc != 0) return -1;

    return 0;
}

RC RelationManager::deleteCatalog()
{
    int tbl_rc = _rbfm->destroyFile("Tables.tbl");
    int col_rc = _rbfm->destroyFile("Columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    int rc = remove("tables.stat");
    if(rc != 0) return -1;    

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
   
   int rc = _rbfm->createFile(tableName + ".tbl");
   if(rc != 0) return -1;
   cout << "test" << endl;

   FileHandle fh;
   rc = _rbfm->openFile("Tables.tbl", fh);
   if(rc != 0) return -1;

   int maxId = getMaxTableId() + 1;
   
   rc = insertTableRecord(fh, maxId, tableName, tableName + ".tbl", TBL_USER);
   cout << "Creating table: " << tableName << " with ID: " << maxId << endl;
   cout << "Insert table rec (crateTable) " << rc << endl;
   if(rc != 0) return -1;
 
   rc = setMaxTableId(maxId);
   if(rc != 0) return -1;

   rc = _rbfm->closeFile(fh);
   if(rc != 0) return -1;

   rc = _rbfm->openFile("Columns.tbl", fh);
   if(rc != 0) return -1;

   for(unsigned int i = 0; i < attrs.size(); ++i) {
      Attribute attr = attrs.at(i);
      rc = insertColumnRecord(fh, maxId, attr.name, attr.type, attr.length, i+1);   
      if(rc != 0) return -1;
   }

   rc = _rbfm->closeFile(fh);
   if(rc != 0) return -1;
   
   return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    FileHandle fh;
    int rc = _rbfm->openFile("Tables.tbl", fh);
    if(rc != 0) {
        _rbfm->closeFile(fh);
        return -1;
    }
    
    RBFM_ScanIterator rbsi;
    const vector<string> tableAttrs ({"table-id", "privileged"});
    
    rc = _rbfm->scan(fh, tablesColumns(), "table-name", EQ_OP, tableName.c_str(), tableAttrs, rbsi);
    if(rc != 0) { 
        _rbfm->closeFile(fh);    
        rbsi.close();
        return -1;
    }
    
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
    if(rbsi.getNextRecord(rid, returnedData) == RM_EOF) {
        _rbfm->closeFile(fh);
        rbsi.close();
        free(returnedData);
        return -1;
    }
    
    
    
    rbsi.close();
    _rbfm->deleteRecord(fh, tablesColumns(), rid);    
    _rbfm->closeFile(fh);
 
    int tableId;
    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
    
    int tableType;
    memcpy(&tableType, (char *) returnedData + 1 + sizeof(int), sizeof(int));
    
    if(tableType == TBL_SYS)  {
        free(returnedData);
        return -1;
    }
   
    const vector<string> colAttrs ({"column-name", "column-type", "column-length"});    
    
    rc = _rbfm->openFile("Columns.tbl", fh);
   
    rc = _rbfm->scan(fh, columnsColumns(), "table-id", EQ_OP, (void *)&tableId, colAttrs, rbsi);
    if(rc != 0) {
        _rbfm->closeFile(fh);
        rbsi.close();
        free(returnedData);
        return -1;
    }
    
    vector<RID> rids;
    
    while(rbsi.getNextRecord(rid, returnedData) != RM_EOF) {
        rids.push_back(rid);
    }
    
    rbsi.close();
    for (const RID &value : rids) {
        _rbfm->deleteRecord(fh, columnsColumns(), value);
    }
    
    _rbfm->closeFile(fh);
    _rbfm->destroyFile(tableName + ".tbl");
    
    free(returnedData);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    FileHandle fh;
    int rc = _rbfm->openFile("Tables.tbl", fh);
    if(rc != 0) {
        _rbfm->closeFile(fh);
        return -1;
    }
    
    RBFM_ScanIterator rbsi;
    const vector<string> tableAttrs ({"table-id"});
    
    rc = _rbfm->scan(fh, tablesColumns(), "table-name", EQ_OP, tableName.c_str(), tableAttrs, rbsi);
    if(rc != 0) { 
       _rbfm->closeFile(fh);    
        rbsi.close();
        return -1;
    }
    
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
    if(rbsi.getNextRecord(rid, returnedData) == RM_EOF) {
        _rbfm->closeFile(fh);
        rbsi.close();
        free(returnedData);
        return -1;
    }
    
    _rbfm->closeFile(fh);
    rbsi.close();
 
    int tableId;
    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
       
    const vector<string> colAttrs ({"column-name", "column-type", "column-length"});    
    
    rc = _rbfm->openFile("Columns.tbl", fh);
   
    rc = _rbfm->scan(fh, columnsColumns(), "table-id", EQ_OP, (void *)&tableId, colAttrs, rbsi);
    if(rc != 0) {
        _rbfm->closeFile(fh);
        rbsi.close();
        free(returnedData);
        return -1;
    }
    
    while(rbsi.getNextRecord(rid, returnedData) != RM_EOF) {
        int offset = 1;
        
        int len;
        memcpy(&len, (char *)returnedData + offset, sizeof(int));
        offset += sizeof(int);

        string tmp;
        tmp.assign((char *)returnedData +  offset, (char *)returnedData + offset + len);
        offset += len;
        
        int type;
        memcpy(&type, (char *)returnedData + offset, sizeof(int));
        offset += sizeof(int);
        
        int length;
        memcpy(&length, (char *)returnedData + offset, sizeof(int));
       
        Attribute attr;
        attr.name = tmp;
        attr.type = (AttrType)type;
        attr.length = (AttrLength)length;
        attrs.push_back(attr); 
    }
    
    rbsi.close();
    _rbfm->closeFile(fh);
    
    free(returnedData);

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{  
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
 
    FileHandle fh;
    rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
 
    rc = _rbfm->insertRecord(fh, attrs,  data, rid);
    if(rc != 0) return -1;
    
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
 
    FileHandle fh;
    rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    rc = _rbfm->deleteRecord(fh, attrs, rid);
    if(rc != 0) return -1;
  
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
 
    FileHandle fh;
    rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    rc = _rbfm->updateRecord(fh, attrs, data, rid);
    if(rc != 0) return -1;
  
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
 
    FileHandle fh;
    rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    rc = _rbfm->readRecord(fh, attrs, rid, data);
    if(rc != 0) return -1;
  
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	int rc = _rbfm->printRecord(attrs, data);
    if(rc != 0) return -1;
    
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
 
    FileHandle fh;
    rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    rc = _rbfm->readAttribute(fh, attrs, rid, attributeName, data);
    if(rc != 0) return -1;

    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
   
    vector<Attribute> attrs;
    int rc = getAttributes(tableName, attrs);
    if(rc != 0) return -1;
    
    string condAttr = conditionAttribute;
    unordered_set<string> givenColumns;
    for(unsigned int i = 0; i < attributeNames.size(); ++i) { 
      givenColumns.insert(attributeNames.at(i));
    }

    unordered_set<string> sysColumns;
    for(unsigned int i = 0; i < attrs.size(); ++i) {
        sysColumns.insert(attrs.at(i).name);
    }
    
    for(auto &col: givenColumns) {
        if(sysColumns.count(col) == 0) return -1;
    }
    
    if(compOp != NO_OP) {
        if(sysColumns.count(conditionAttribute) == 0) return -1;
    } else {
        condAttr = attrs.at(0).name;
    }
 
    
    ///////////////////////
    // check if attributes exist
    // check if conditionAttribute exists (if not NO_OP)
    ///////////////////////
    
    FileHandle *fh = new FileHandle();
    rc = _rbfm->openFile(tableName + ".tbl", *fh);
    if(rc != 0) return -1;

    RBFM_ScanIterator *rbfmScanIterator = new RBFM_ScanIterator();
    
    rc = _rbfm->scan(*fh, attrs, condAttr, compOp, value, attributeNames, *rbfmScanIterator);
    if(rc != 0) {
        delete fh;
        delete rbfmScanIterator;
        return -1;
    }
    rm_ScanIterator.rbfmScanIterator = rbfmScanIterator; 
    rm_ScanIterator.fileHandle = fh;

    return 0;
}


RC RelationManager::prepareColumnRecord(const int tableId, const int nameLength, const string &name, const int colType, const int colLength, const int colPos, void *buffer, int *tupleSize) {

   int offset = 0;
   int nullAttributesIndicatorActualSize = 1;
   unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
   memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);   

   memcpy((char *)buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
   offset += nullAttributesIndicatorActualSize;
   
   memcpy((char *)buffer + offset, &tableId, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, &nameLength, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, name.c_str(), nameLength);
   offset += nameLength;

   memcpy((char *)buffer + offset, &colType, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, &colLength, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, &colPos, sizeof(int));
   offset += sizeof(int);

   *tupleSize = offset;
   free(nullsIndicator);

   return 0;
}

RC RelationManager::prepareTableRecord(const int tableId, const int nameLength, const string &name, const int fileLength, const string &file, const int privileged, void *buffer, int *tupleSize) {

   int offset = 0;
   int nullAttributesIndicatorActualSize = 1;
   unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
   memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);   

   memcpy((char *)buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
   offset += nullAttributesIndicatorActualSize;
   
   memcpy((char *)buffer + offset, &tableId, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, &nameLength, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, name.c_str(), nameLength);
   offset += nameLength;

   memcpy((char *)buffer + offset, &fileLength, sizeof(int));
   offset += sizeof(int);

   memcpy((char *)buffer + offset, file.c_str(), fileLength);
   offset += fileLength;

   memcpy((char *)buffer + offset, &privileged, sizeof(int));
   offset += sizeof(int);

   *tupleSize = offset;
   free(nullsIndicator);

   return 0;
}

RC RelationManager::insertTableRecords() {
  
   FileHandle fh;
   int rc = _rbfm->openFile("Tables.tbl", fh);
   if(rc != 0) return -1;

   int tableId = getMaxTableId() + 1;
   rc = insertTableRecord(fh, tableId, "Tables", "Tables.tbl", TBL_SYS);
   if(rc != 0) return -1;

   tableId += 1;
   rc = insertTableRecord(fh, tableId, "Columns", "Columns.tbl", TBL_SYS);
   if(rc != 0) return -1;

   rc = _rbfm->closeFile(fh);
   if(rc != 0) return -1;

   rc = setMaxTableId(tableId);
   if(rc != 0) return -1;
   
   return 0;
}

RC RelationManager::insertColumnRecords() {
   
   FileHandle fh;
   int rc = _rbfm->openFile("Columns.tbl", fh);
   if(rc != 0) return -1;

   rc = insertColumnRecord(fh, 1, "table-id", TypeInt, 4, 1);
   if(rc != 0) return -1;

   rc = insertColumnRecord(fh, 1, "table-name", TypeVarChar, 50, 2);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 1, "file-name", TypeVarChar, 50, 3);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 1, "privileged", TypeInt, 4, 4);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 2, "table-id", TypeInt, 4, 1);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 2, "column-name", TypeVarChar, 50, 2);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 2, "column-type", TypeInt, 4, 3);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 2, "column-length", TypeInt, 4, 3);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 2, "column-position", TypeInt, 4, 4);
   if(rc != 0) return -1;

   rc = _rbfm->closeFile(fh);
   if(rc != 0) return -1;
   
   return 0;
}

vector<Attribute> RelationManager::tablesColumns() {
   vector<Attribute> recordDescriptor;
   Attribute attr;
   
   attr.name = "table-id";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);

   attr.name = "table-name";
   attr.type = TypeVarChar;
   attr.length = (AttrLength)50;
   recordDescriptor.push_back(attr);
   
   attr.name = "file-name";
   attr.type = TypeVarChar;
   attr.length = (AttrLength)50;
   recordDescriptor.push_back(attr);
   
   attr.name = "privileged";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);

   return recordDescriptor;
}


vector<Attribute> RelationManager::columnsColumns() {
   vector<Attribute> recordDescriptor;
   Attribute attr;
   
   attr.name = "table-id";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);

   attr.name = "column-name";
   attr.type = TypeVarChar;
   attr.length = (AttrLength)50;
   recordDescriptor.push_back(attr);
   
   attr.name = "column-type";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);
   
   attr.name = "column-length";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);
   
   attr.name = "column-position";
   attr.type = TypeInt;
   attr.length = (AttrLength)4;
   recordDescriptor.push_back(attr);
   
   return recordDescriptor;
}

RC RelationManager::insertTableRecord(FileHandle &fh, const int tableId, const string name, const string fileName, const int privileged) {

   RID rid;
   vector<Attribute> recordDescriptor = tablesColumns();
   
   int recordSize = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + fileName.length() + sizeof(int);
   
   void *record = malloc(size);
   prepareTableRecord(tableId, name.length(), name, fileName.length(), fileName, privileged, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   if(memcmp(record, returnedData, recordSize) != 0) {
      cout << "FATAL ERROR" << endl;
   }
   
   free(returnedData);
   free(record);
   
   return 0;
}


RC RelationManager::insertColumnRecord(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   vector<Attribute> recordDescriptor = columnsColumns();
   
   int recordSize = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int);
   
   void *record = malloc(size);

   prepareColumnRecord(tableId, name.length(), name, colType, colLength, colPos, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   if(memcmp(record, returnedData, recordSize) != 0) {
       cout << "FATAL ERROR" << endl;
   }
   free(returnedData);
   free(record);
   
   return 0;
}

int RelationManager::getMaxTableId() {
   int maxId;
   int rc = fseek(stats, 0, SEEK_SET);
   if(rc != 0) return -1;

   rc = fread(&maxId, sizeof(int), 1, stats);
   if(rc == 0) maxId = 0;
   
   return maxId;
}

RC RelationManager::setMaxTableId(int maxId) {
   int rc = fseek(stats, 0, SEEK_SET);
   if (rc != 0) return -1;

   rc = fwrite(&maxId, sizeof(int), 1, stats);
   if (rc != 1) return -1;
   
   rc = fflush(stats);
   if(rc != 0) return -1;

   return 0;
}
