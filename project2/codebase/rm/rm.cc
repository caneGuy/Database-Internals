
#include "rm.h"

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
   stats = fopen("tables.stat", "ab+");
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
    //FileHandle fh;
    //int rc = _rbfm->openFile("tables.tbl", fh);
    //if(rc != 0) return -1;
    
   
    int tbl_rc = _rbfm->destroyFile("tables.tbl");
    int col_rc = _rbfm->destroyFile("columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    FILE* rc = fopen("tables.stat", "w");
    if(rc == NULL) return -1;    

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
   cout << "Insert table rec (crateTable) " << rc << endl;
   if(rc != 0) return -1;
 
   maxId += 1;
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
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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
   cout << "Finished prepareColumnRecord" << endl;   

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

   cout << "Privlege byte: " << privileged << endl; 
   memcpy((char *)buffer + offset, &privileged, sizeof(int));
   offset += sizeof(int);

   *tupleSize = offset;
   cout << "Finished prepareTableRecord" << endl;   

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
   cout << "Set max id: " << rc << endl;
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
   
   rc = insertColumnRecord(fh, 1, "column-length", TypeInt, 4, 3);
   if(rc != 0) return -1;
   
   rc = insertColumnRecord(fh, 1, "column-position", TypeInt, 4, 4);
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
   cout << "After malloc of record" << endl;
   prepareTableRecord(tableId, name.length(), name, fileName.length(), fileName, privileged, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;
   cout << "After insert record" << endl;   

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   cout << "After read record" << endl;
   if(memcmp(record, returnedData, recordSize) != 0) {
      cout << "FATAL ERROR" << endl;
   }

   cout << "Inserted start" << endl; 
   const char* p = reinterpret_cast< const char *>(record);
   for ( unsigned int i = 0; i < size; i++ ) {
      std::cout << hex << int(p[i]) << " ";
   }
   cout << endl << "Inserted end" << endl;

   cout << "Inserted start" << endl; 
   p = reinterpret_cast< const char *>(returnedData);
   for ( unsigned int i = 0; i < size; i++ ) {
      std::cout << hex << int(p[i]) << " ";
   }
   cout << endl << "Inserted end" << endl;
   
   return 0;
}


RC RelationManager::insertColumnRecord(FileHandle &fh, const int tableId, const string name, const int colType, const int colLength, const int colPos) {

   RID rid;
   vector<Attribute> recordDescriptor = columnsColumns();
   
   int recordSize = 0;
   size_t size = 1 + sizeof(int) + sizeof(int) + name.length() + sizeof(int) + sizeof(int) + sizeof(int);
   
   void *record = malloc(size);
   cout << "After malloc of record" << endl;

   prepareColumnRecord(tableId, name.length(), name, colType, colLength, colPos, record, &recordSize);
   int rc = _rbfm->insertRecord(fh, recordDescriptor, record, rid);
   if(rc != 0) return -1;
   cout << "After insert record" << endl;   

   void *returnedData = malloc(size);
   _rbfm->readRecord(fh, recordDescriptor, rid, returnedData);
   cout << "After read record" << endl;
   if(memcmp(record, returnedData, recordSize) != 0) {
      cout << "FATAL ERROR" << endl;
   }

   cout << "Inserted start" << endl; 
   const char* p = reinterpret_cast< const char *>(record);
   for ( unsigned int i = 0; i < size; i++ ) {
      std::cout << hex << int(p[i]) << " ";
   }
   cout << endl << "Inserted end" << endl;

   cout << "Inserted start" << endl; 
   p = reinterpret_cast< const char *>(returnedData);
   for ( unsigned int i = 0; i < size; i++ ) {
      std::cout << hex << int(p[i]) << " ";
   }
   cout << endl << "Inserted end" << endl;
   
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

   rc = fwrite(&maxId, sizeof(int), 1, stats);
   if (rc != 1) return -1;
   
   rc = fflush(stats);
   if(rc != 0) return -1;

   return 0;
}
