
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
}

RelationManager::~RelationManager()
{
   _rbfm = NULL;
}

RC RelationManager::createCatalog()
{
    int tbl_rc = _rbfm->createFile("tables.tbl");
    int col_rc = _rbfm->createFile("columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    cout << "Created catalog tbl files" << endl;
    tbl_rc = insertTableRecords();
    col_rc = insertColumnRecords();
    if(tbl_rc != 0 || col_rc != 0) return -1;

    return 0;
}

RC RelationManager::deleteCatalog()
{
    int tbl_rc = _rbfm->destroyFile("tables.tbl");
    int col_rc = _rbfm->destroyFile("columns.tbl");
    if(tbl_rc != 0 || col_rc != 0) return -1;

    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
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

   cout << "File: " << file << endl;
   memcpy((char *)buffer + offset, file.c_str(), fileLength);
   offset += fileLength;

   cout << "privlege bit: " << privileged << endl; 
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

   rc = insertTableRecord(fh, 1, "tables", "tables.tbl", TBL_SYS);
   if(rc != 0) return -1;
   cout << "Inserted tables record into tables.tbl file" << endl;

   rc = insertTableRecord(fh, 2, "columns", "columns.tbl", TBL_SYS);
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
