#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <iostream>
#include <cstring>
#include <unordered_set>

#include "../rbf/pfm.h"

using namespace std;

const int16_t deleted_entry = 0x8000;

typedef unsigned AttrLength;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;

typedef enum { 
    TypeInt = 0, 
    TypeReal, 
    TypeVarChar 
} AttrType;

typedef struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
} Attribute_t;

typedef enum { 
    EQ_OP = 0, // no condition// = 
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


# define RBFM_EOF (-1)  // end of a scan operator

class RBFM_ScanIterator {
    
    friend class RecordBasedFileManager;
    
    public:
        RBFM_ScanIterator() { };
        ~RBFM_ScanIterator() {};
        RC getNextRecord(RID &rid, void *data);
        RC close();
        
    private:
        FileHandle fh;
        vector<Attribute> recordDescriptor;
        uint16_t conditionAttributeIndex;
        CompOp compOp;
        void *value;
        unordered_set<uint16_t> index;  
        char *page;
        int curr_page;
        uint16_t curr_slot;
        uint16_t max_slots;
        uint16_t attributeNamesCount;
        
        uint16_t vc_comp(string val);
        uint16_t int_comp(int val);
        uint16_t float_comp(float val);
        
};


class RecordBasedFileManager
{
    public:
        static RecordBasedFileManager* instance();

        RC createFile(const string &fileName);

        RC destroyFile(const string &fileName);

        RC openFile(const string &fileName, FileHandle &fileHandle);

        RC closeFile(FileHandle &fileHandle);

        RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

        RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

        RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

        RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

        RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

        // Scan sets up an iterator to allow the caller to go through the results one by one. 
        RC scan(FileHandle &fileHandle,
                const vector<Attribute> &recordDescriptor,
                const string &conditionAttribute,
                const CompOp compOp,                  
                const void *value,                    
                const vector<string> &attributeNames,
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        ~RecordBasedFileManager();
        RecordBasedFileManager();

    private:
        static RecordBasedFileManager *_rbf_manager;


        RC tryInsert(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
        
        uint16_t makeRecord(const vector<Attribute> &recordDescriptor, const void *data, char *record);
  
};

#endif
