
#include "rm.h"

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    int rc = rbfmScanIterator->getNextRecord(rid, data);
    cout << "__getNextRecord: " << rc << endl;
    if(rc != 0) return RM_EOF;

    return 0;
}

RC RM_ScanIterator::close() {
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
   stats = fopen("tables.stat", "wrb+");
   assert(stats != NULL);
}

RelationManager::~RelationManager()
{
   _rbfm = NULL;
   fflush(stats);
   fclose(stats);
}

RC RelationManager::createCatalog()
{
    int tbl_rc = _rbfm->createFile("tables.tbl");
    int col_rc = _rbfm->createFile("columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    cout << "Created catalog tbl files" << endl;
    tbl_rc = insertTableRecords();
    col_rc = insertColumnRecords();
    cout << "insertTableRecords: " << tbl_rc << " insertColumnRecords: " << col_rc << endl;
    if(tbl_rc != 0 || col_rc != 0) return -1;

    return 0;
}

RC RelationManager::deleteCatalog()
{
    int tbl_rc = _rbfm->destroyFile("tables.tbl");
    int col_rc = _rbfm->destroyFile("columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    int rc = remove("tables.stat");
    if(rc != 0) return -1;    

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
   int rc = _rbfm->createFile(tableName + ".tbl");
   cout << "Created " << (tableName + ".tbl: ") << rc << endl; 
   if(rc != 0) return -1;

   FileHandle fh;
   rc = _rbfm->openFile("tables.tbl", fh);
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

   rc = _rbfm->openFile("columns.tbl", fh);
   if(rc != 0) return -1;

   for(unsigned int i = 0; i < attrs.size(); ++i) {
      Attribute attr = attrs.at(i);
      rc = insertColumnRecord(fh, maxId, attr.name, attr.type, attr.length, i+1);   
      cout << "Create table insert col rec: " << rc << endl;
      if(rc != 0) return -1;
   }

   rc = _rbfm->closeFile(fh);
   if(rc != 0) return -1;
   
   return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    FileHandle fh;
    int rc = _rbfm->destroyFile(tableName + ".tbl");
    if(rc != 0) return -1;

    rc = _rbfm->openFile("tables.tbl", fh);
    if(rc != 0) return -1;

    cout << "Opened tables.tbl" << endl;
 
    RM_ScanIterator *rmsi = new RM_ScanIterator();
    const vector<string> tableAttrs ({"table-id"});

    cout << "************deleteTable:" << tableName << endl;
    rc = scan("tables", "table-name", EQ_OP, tableName.c_str(), tableAttrs, *rmsi);
    cout << "___!__Scan result: " << rc << endl;
    if(rc != 0) return -1;
    
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);

    cout << "FUCK" << endl;
    if(rmsi->getNextTuple(rid, returnedData) == RM_EOF) return -1;

    int tableId;
    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
    cout << "wow Table: (" << tableName << ", " << tableId << ")"  << endl;

    if(deleteTuple("tables", rid) != 0) return -1;
    cout << "After deleting the rid of table" << endl;   
 
    const vector<string> colAttrs ({"column-name"});
    rc = scan("columns", "table-id", EQ_OP, (void *)&tableId, colAttrs, *rmsi);
    if(rc != 0) return -1;

    while(rmsi->getNextTuple(rid, returnedData) != RM_EOF) {
        if(deleteTuple("columns", rid) != 0) return -1;
    }
            
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    cout << "GET_ATTRIBUTES " << endl;
    FileHandle fh;
    int rc = _rbfm->openFile("tables.tbl", fh);
    if(rc != 0) return -1;
    
    RM_ScanIterator rmsi;
    const vector<string> tableAttrs ({"table-id"});
    
    rc = scan("tables", "table-name", EQ_OP, tableName.c_str(), tableAttrs, rmsi);
    if(rc != 0) return -1;
    
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
    cout << "BEFORE_GET_ATTRIBUTES_GET_NEXT_TUPLE*******" << endl;
    if(rmsi.getNextTuple(rid, returnedData) == RM_EOF) return -1;
 
    int tableId;
    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
    cout << "Table: (" << tableName << ", " << tableId << ") ---------------"  << endl;
   
    const vector<string> colAttrs ({"column-name", "column-type", "column-length"});
   
    rc = scan("columns", "table-id", EQ_OP, (void *)&tableId, colAttrs, rmsi);
    if(rc != 0) return -1;
    
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        int offset = 1;
        
        int len;
        memcpy(&len, (char *)returnedData + offset, sizeof(int));
        offset += sizeof(int);

        string tmp;
        tmp.assign((char *)returnedData +  offset, (char *)returnedData + offset + len);
        cout << "!++!+!Column: " << tmp << endl;
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

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
   
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
 
    rc = _rbfm->insertRecord(fh, attrs,  data, rid);
    if(rc != 0) return -1;
    
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    rc = _rbfm->deleteRecord(fh, attrs, rid);
    if(rc != 0) return -1;
  
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    rc = _rbfm->updateRecord(fh, attrs, data, rid);
    if(rc != 0) return -1;
  
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
    
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
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
    FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;
   
     
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
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
    // get record descriptor for this table
    // check that the columns in the predicate or projection are in record descriptor

    // setup iterator for tables table (and then columns table with that id)
     
    RBFM_ScanIterator *rbfmScanIterator = new RBFM_ScanIterator();
    
    FileHandle fh;
	int rc = _rbfm->openFile("tables.tbl", fh);
	if(rc != 0) return -1;

    cout << "In scan, opened tables.tbl" << endl;
    cout << "scanning for table: " << tableName << endl;
    cout << "value*: " << (char*) value << endl;
    cout << "value (int): " << (int*) value << endl;

	const vector<string> tblAttrs ({"table-id", "table-name", "file-name", "privileged"});
	rc = _rbfm->scan(fh, tablesColumns(), "table-name", EQ_OP, tableName.c_str(), tblAttrs, *rbfmScanIterator);
    rm_ScanIterator.rbfmScanIterator = rbfmScanIterator; 
    if(rc != 0) return -1;

	RID rid;
	void *returnedData = malloc(PAGE_SIZE);

	unordered_set<string> givenColumns ({conditionAttribute});
	for(unsigned int i = 0; i < attributeNames.size(); ++i) { 
	  givenColumns.insert(attributeNames.at(i));
	}

	unordered_set<string> sysColumns;
	cout << "Right before RM_SI get next tuple" << endl;
    if(rm_ScanIterator.getNextTuple(rid, returnedData) == RM_EOF) return -1;
    cout << "BEFORE_PRINT TUPLE " << endl;
    printTuple(tablesColumns(), returnedData);
    
    int tableId;
    memcpy(&tableId, (char *)returnedData + 1, sizeof(int));
    cout << "Table: (" << tableName  << ", " << tableId << ")"  << endl;

    rc = _rbfm->closeFile(fh);
    if(rc != 0) return -1;

    rc = _rbfm->openFile("columns.tbl", fh);
    if(rc != 0) return -1;

    const vector<string> colAttrs ({"column-name", "column-type", "column-length"});
    rc = _rbfm->scan(fh, columnsColumns(), "table-id", EQ_OP, &tableId, colAttrs, *rbfmScanIterator);
    rm_ScanIterator.rbfmScanIterator = rbfmScanIterator;
    if(rc != 0) return -1;

    cout << "Setup COLUMN_ITERATOR" << endl;
    vector<Attribute> descriptor = columnsColumns();
    descriptor.erase(descriptor.begin());
    descriptor.erase(descriptor.end() - 1);   
 
    vector<Attribute> recordDescriptor;
    while(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        int offset = 1;

        printTuple(descriptor, returnedData);
        
        int len;
        memcpy(&len, (char *)returnedData + offset, sizeof(int));
        offset += sizeof(int);

        string tmp;
        tmp.assign((char *)returnedData +  offset, (char *)returnedData + offset + len);
        cout << "Column test: " << tmp << endl;
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
        recordDescriptor.push_back(attr); 
        cout << "REC DESC: " << attr.name << " " << attr.type << " " << attr.length << endl;
    }

    cout << "Final rec descriptor: " << endl;
    for(int i = 0; i < recordDescriptor.size(); ++ i) {
        Attribute attr = recordDescriptor.at(i);
        cout << i << " " << attr.name << " " << attr.type << " " << attr.length << endl;
    }

    rc = _rbfm->closeFile(fh);
    if(rc != 0) return -1;
  
    FileHandle *fileH = new FileHandle(); 
    cout << "Openeing file table: " << tableName << endl; 
    rc = _rbfm->openFile(tableName + ".tbl", *fileH);
    if(rc != 0) return -1;
  
    cout << "Setup RD... " << endl;
    cout << " condAttr: " << conditionAttribute << endl;
    cout << "where it equals: " << (char*) value << endl; 
    
    rc = _rbfm->scan(*fileH, recordDescriptor, conditionAttribute, compOp, value, attributeNames, *rbfmScanIterator);
    rm_ScanIterator.rbfmScanIterator = rbfmScanIterator; 
    if(rc != 0) return -1;
   
    cout << "RBFM__SCAN: " << rc << endl; 
    // compare attribute names and conditionattribute to recorddescriptor names   
 
    /*int offset = 1;
    int tableId;
    memcpy(&tableId, (char *)returnedData + offset, sizeof(int));
    */

    /*cout << "After EOF getnextTuple" << endl;
	int offset = 1;
	char *ptr = (char *)returnedData;

	int len;
	memcpy(&len, ptr + 1, sizeof(int));
	offset += sizeof(int);

	string tmp;
	tmp.assign(ptr[offset], ptr[offset] + len);
	cout << "Column: " << tmp << endl;

	sysColumns.insert(tmp);*/
    // null byte, var-char length (4), value
    
    /*FileHandle fh;
    int rc = _rbfm->openFile(tableName + ".tbl", fh);
    if(rc != 0) return -1;*/


    //rc = _rbfm->scan(fh, tablesColumns(), conditionAttribute, compOp, value, attributeNames, rbfmScanIterator);

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
   //cout << "Finished prepareColumnRecord" << endl;   

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

   //cout << "Privlege byte: " << privileged << endl; 
   memcpy((char *)buffer + offset, &privileged, sizeof(int));
   offset += sizeof(int);

   *tupleSize = offset;
   //cout << "Finished prepareTableRecord" << endl;   

   return 0;
}

RC RelationManager::insertTableRecords() {
  
   FileHandle fh;
   int rc = _rbfm->openFile("tables.tbl", fh);
   if(rc != 0) return -1;
   cout << "Opened tables.tbl fh" << endl;

   int tableId = getMaxTableId() + 1;
   rc = insertTableRecord(fh, tableId, "tables", "tables.tbl", TBL_SYS);
   if(rc != 0) return -1;
   cout << "Inserted tables record into tables.tbl file" << endl;

   tableId += 1;
   rc = insertTableRecord(fh, tableId, "columns", "columns.tbl", TBL_SYS);
   if(rc != 0) return -1;

   rc = _rbfm->closeFile(fh);
   cout << "Close file: " << rc << endl;
   if(rc != 0) return -1;

   rc = setMaxTableId(tableId);
   cout << "Set max id: " << tableId << endl;
   if(rc != 0) return -1;
   
   return 0;
}

RC RelationManager::insertColumnRecords() {
   
   FileHandle fh;
   int rc = _rbfm->openFile("columns.tbl", fh);
   if(rc != 0) return -1;
   cout << "Opened columns.tbl fh" << endl;

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
   
   cout << "Inserted cols record into cols.tbl file" << endl;
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
   // cout << "After malloc of record" << endl;
   prepareTableRecord(tableId, name.length(), name, fileName.length(), fileName, privileged, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;
   // cout << "After insert record" << endl;   

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   // cout << "After read record" << endl;
   if(memcmp(record, returnedData, recordSize) != 0) {
      // cout << "FATAL ERROR" << endl;
   }

   // cout << "Inserted start" << endl; 
   //const char* p = reinterpret_cast< const char *>(record);
   for ( unsigned int i = 0; i < size; i++ ) {
      // std::cout << hex << int(p[i]) << " ";
   }
   // cout << endl << "Inserted end" << endl;

   // cout << "Inserted start" << endl; 
   //p = reinterpret_cast< const char *>(returnedData);
   for ( unsigned int i = 0; i < size; i++ ) {
      // std::cout << hex << int(p[i]) << " ";
   }
   // cout << endl << "Inserted end" << endl;
   
   return 0;
}


RC RelationManager::insertColumnRecord(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   vector<Attribute> recordDescriptor = columnsColumns();
   
   int recordSize = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int);
   
   void *record = malloc(size);
   // cout << "After malloc of record" << endl;

   prepareColumnRecord(tableId, name.length(), name, colType, colLength, colPos, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;
   // cout << "After insert record" << endl;   

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   // cout << "After read record" << endl;
   if(memcmp(record, returnedData, recordSize) != 0) {
      // cout << "FATAL ERROR" << endl;
   }

   // cout << "Inserted start" << endl; 
   //const char* p = reinterpret_cast< const char *>(record);
   for ( unsigned int i = 0; i < size; i++ ) {
      // std::cout << hex << int(p[i]) << " ";
   }
   // cout << endl << "Inserted end" << endl;

   // cout << "Inserted start" << endl; 
   //p = reinterpret_cast< const char *>(returnedData);
   for ( unsigned int i = 0; i < size; i++ ) {
      // std::cout << hex << int(p[i]) << " ";
   }
   // cout << endl << "Inserted end" << endl;
   
   return 0;
}

int RelationManager::getMaxTableId() {
   int maxId;
   int rc = fseek(stats, 0, SEEK_SET);
   if(rc != 0) return -1;

   rc = fread(&maxId, sizeof(int), 1, stats);
   if(rc == 0) maxId = 0;
   
   cout << "Max table ID: " << maxId << endl; 
   return maxId;
}

RC RelationManager::setMaxTableId(int maxId) {
   int rc = fseek(stats, 0, SEEK_SET);
   if (rc != 0) return -1;

   cout << "writing: " << maxId << endl;
   rc = fwrite(&maxId, sizeof(int), 1, stats);
   if (rc != 1) return -1;
   
   rc = fflush(stats);
   if(rc != 0) return -1;

   return 0;
}
