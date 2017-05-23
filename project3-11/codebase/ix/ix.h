#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan
#define ROOT_PAGE (0) 

class IX_ScanIterator;
class IXFileHandle;

struct nodeHeader {
    uint8_t leaf;
    uint16_t pageNum;
    uint16_t freeSpace;
    uint16_t left;
    uint16_t right;
};

// to your left there is less or equal
// to your right there is strictly greater

struct interiorEntry {
    uint16_t left;
    uint16_t right;
    Attribute attribute;
    char key[PAGE_SIZE];
};

struct leafEntry {
    Attribute attribute;
    char key[PAGE_SIZE];
    RID rid;
    uint16_t sizeOnPage;
};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        static PagedFileManager *_pf_manager;
        
        
        RC insertRec(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, vector<uint16_t> pageStack);
        RC insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* const key, RID rid, vector<uint16_t> pageStack);
        RC insertToInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t oldPage, uint16_t newPage, vector<uint16_t> pageStack); 
        void print_rec(uint16_t depth, uint16_t pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        struct interiorEntry nextInteriorEntry(char* page, Attribute attribute, uint16_t &offset) const;
        struct leafEntry nextLeafEntry(char* page, Attribute attribute, uint16_t &offset) const; 
        uint16_t getSize(const Attribute &attribute, const void* key) const;
        RC isKeySmaller(const Attribute &attribute, const void* pageEntryKey, const void* key);
        void printLeafEntry(struct leafEntry entry) const;
        void printKey(const Attribute& attribute, void* key) const;
        RC createNewRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t page2Num);
        void hexdump(const void *ptr, int buflen);
};


    // don't delete this yet pls
    // set up new header, and update the other one
    // walk down the list until approximatly in the middle
    // copy half of the data into the new page (don't forget to update freeSpace)
    // last entry on left page will be trafficCup
    // overwrite page, append page2 (remember "page2Num")
    // call inserts
    
    
    // split node
    // if coded properly we might be able to use almost the same code in insertToLeaf
    // note: there will be a recursive call to insertToInterior()
    // need to catch special case, if we want to split the root
    // root has to stay at page 0
    // so append both halves of the root node at the end of the file
    // set up new root page with one trafficCup and save it at page 0


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:
    
        FileHandle *_fileHandle;

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    
 
};

#endif
