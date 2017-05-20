#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager* IndexManager::_pf_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager) {
        _index_manager = new IndexManager();
    }
  
    return _index_manager;
}

IndexManager::IndexManager()
{
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    int rc =  _pf_manager->createFile(fileName);
    if(rc != 0) return -1;

    struct nodeHeader header;
    header.leaf = 0;
    header.pageNum = 0;
    header.freeSpace = sizeof(struct nodeHeader);
    header.left = -1;
    header.right = -1;
    
    void *data = malloc(PAGE_SIZE);
    memcpy(data, &header, sizeof(struct nodeHeader));
    
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(data))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(data);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName, *(ixfileHandle._fileHandle));
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(*(ixfileHandle._fileHandle));
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{    
    insertRec(ROOT_PAGE, ixfileHandle, attribute, key, rid);    
    return 0;
}

RC insertRec(uint16_t pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, uint16_t parentPageNum)
{    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    
    if (pageHeader.leaf == true) {    
        free(page);    
        return insertToLeaf(pageNum, attribute, key, rid, parentPageNum);        
    } else {              
        uint16_t offset = sizeof(struct nodeHeader);   
        uint16_t nextPageNum;  
        memcpy(&nextPageNum, page + offset, sizeof(uint16_t));     
        struct interiorEntry entry;
        // walk through the page until key is not smaller any more 
        // or we reached the last entry 
        // (+sizeof(uint16_t) because offset points at the beinning of the next entry's left pointer)
        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            if (not isKeySmaller(attribute, entry.key, key)) {
                nextPageNum = entry.left;
                break;
            } else {
                nextPageNum = entry.right;
            }
        }  
        return insertRec(nextPageNum, ixfileHandle, attribute, key, rid, pageNum);
    }
}

RC insertToLeaf(uint16_t pageNum, Attribute attribute, char* key, RID rid, uint16_t parentPageNum){
    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    
    uint16_t keySize = getSize(attribute, key);
    
    // normal insert
    if (pageHeader.freeSpace + keySize + sizeof(RID) <= PAGE_SIZE) {        
        uint16_t offset = sizeof(struct nodeHeader);   
        uint16_t last = offset;      
        struct leafEntry entry;
        // walk through the page until key is not smaller any more 
        // or we reached the last entry 
        while (pageHeader.freeSpace > offset) {
            entry = nextLeafEntry(page, attribute, offset);
            if (not isKeySmaller(attribute, entry.key, key))
                break;
            last = offset;
        }
        // memmove the entries that are not smaller than key
        // memcpy in the key and rid
        memmove(page + last + keySize + sizeof(RID), page + last, pageHeader.freeSpace - last);
        memcpy(page + last, key, keySize);
        memcpy(page + last + keySize, &rid, sizeof(RID));
        // update pageHeader
        pageHeader.freeSpace += keySize + sizeof(RID);
        memcpy(page, &pageHeader, sizeof(struct nodeHeader);
        ixfileHandle->_fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    } 
    
    // try to push one into sibling?
    // lowest priority for now
    
    
    // split node   
    
    // set up new header, and update the other one
    // walk down the list until approximatly in the middle
    // copy half of the data into the new page (don't forget to update freeSpace)
    // last entry on left page will be trafficCup
    // overwrite page, append page2 (remember "page2Num")
    // call inserts
    
    char* page2 = malloc(PAGE_SIZE);    
    uint16_t offset = sizeof(struct nodeHeader);     
    struct leafEntry entry;
    // walk through the page until we are about half way through the page
    while (offset - (sizeof(struct nodeHeader) / 2) < PAGE_SIZE / 2) {
        entry = nextLeafEntry(page, attribute, offset);
    }    
    memcpy(page2 + sizeof(struct nodeHeader), page + offset, pageHeader.freeSpace - offset); 
    // set up headers
    struct nodeHeader page2Header;
    page2Header.freeSpace = sizeof(struct nodeHeader) + pageHeader.freeSpace - offset;
    pageHeader.freeSpace = offset;
    page2Header.leaf = true;
    page2Header.pageNum = ixfileHandle->_fileHandle->getNumberOfPages();
    page2Header.left = pageHeader.pageNum;
    page2Header.right = pageHeader.right;
    pageHeader.right = page2Header.pageNum;
    // write new headers
    memcpy(page, &pageHeader, sizeof(struct nodeHeader));
    memcpy(page2, &page2Header, sizeof(struct nodeHeader));
    ixfileHandle->_fileHandle->writePage(pageNum, page);
    ixfileHandle->_fileHandle->appendPage(page2);
    // insert new trafficCup into parent
    insertToInterior(parentPageNum, attribute, entry.key, page2Header.pageNum);
    // should probably work with parent
    insertRec(ROOT_PAGE, ixfileHandle, attribute, key, rid);      
    
}


RC insertToInterior(pageNum, attribute, trafficCup, page2Num) {
    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    
    uint16_t size = getSize(attribute, key);
    
    // normal insert
    if (pageHeader.freeSpace + getSize(attribute, key) + sizeof(uint16_t) <= PAGE_SIZE) {
        
        // find correct spot by walking down the list
        // then insert (memmove) key, page2Num
        // update header
 
        memcpy(page, &pageHeader, sizeof(struct nodeHeader);
        ixfileHandle->_fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    } 
    
    // split node
    // if coded properly we might be able to use almost the same code in insertToLeaf
    // note: there will be a recursive call to insertToInterior()
    // need to catch special case, if we want to split the root
    // root has to stay at page 0
    // so append both halves of the root node at the end of the file
    // set up new root page with one trafficCup and save it at page 0
    
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) 
{

    print_rec(0, ROOT_PAGE, ixfileHandle, attribute);

}

print_rec(uint16_t depth, uint16_t pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) {
    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    
    if (pageHeader.leaf == true) {
    
        cout << string(depth * 4, ' ') << "{\"keys\": [";
        
        // grep next key
        // print all rid's with that key
        // repeat until reach freeSpace
        
        cout << "]}";
        
    } else {
        
        cout << string(depth * 4, ' ') << "{\"keys\": [";
        
        uint16_t offset = sizeof(struct header);
        while (offset < pageHeader.freeSpace) {
            
        }
        
        cout << "]," << endl;
        
        cout << string(depth * 4, ' ') << " \"children\": ["
        
        offset = sizeof(struct header);
        while (offset < pageHeader.freeSpace) {
            
        } 
        
    }
    
}



struct interiorEntry nextInteriorEntry(char* page, Attribute attribute, uint16_t &offset)  {
    
    struct interiorEntry entry;
    entry.attribute = attribute;
    memcpy(&entry.left, page + offset, sizeof(uint16_t));    
    uint16_t size = getSize(attribute, page + offset + sizeof(uint16_t));    
    memcpy(&entry.key, page + offset + sizeof(uint16_t), size);
    memcpy(&entry.right, page + offset + sizeof(uint16_t) + size, sizeof(uint16_t));
    offset += sizeof(uint16_t) + size;
    return entry;
    
}

struct leafEntry nextLeafEntry(char* page, Attribute attribute, uint16_t &offset) {
    
    struct leafEntry entry;  
    entry.attribute = attribute;
    uint16_t size = getSize(attribute, page + offset);    
    memcpy(&entry.key, page + offset, size);
    memcpy(&entry.rid, page + offset + size, sizeof(RID));
    offset += size + sizeof(RID);
    return entry;
    
}

uint16_t getSize(Attribute, attribute, char* key) {
    
    switch (attribute.type) {
        case TypeVarChar:
            int size;
            memcpy(&size, key, sizeof(int));
            return 4 + size;
        case TypeVarInt:
            return 4;
        case TypeVarFloat:
            return 4;
        default:
            return -1;
    }
    
}

RC isKeySmaller(Attribute attribute, char* pageEntryKey, char* key) {   

    switch (attribute.type) {
        case TypeVarChar:
            int size;
            memcpy(&size, pageEntryKey, sizeof(int));
            pageEntryKey[size] = NULL;
            memcpy(&size, key, sizeof(int));
            key[size] = NULL;
            return strcmp(key + 4, pageEntryKey + 4) >= 0;
        case TypeVarInt:
            int pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(int));
            memcpy(&searchKey, key, sizeof(int));
            return (pageEntryKey < searchKey);
        case TypeVarFloat:
            float pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(float));
            memcpy(&searchKey, key, sizeof(float));
            return (pageEntryKey < searchKey);
        default:
            return -1;
    }
    
}




////////////////////////////////////////////////////////////////////////
// IX_ScanIterator 
////////////////////////////////////////////////////////////////////////

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}

////////////////////////////////////////////////////////////////////////
// IXFileHandle 
////////////////////////////////////////////////////////////////////////

IXFileHandle::IXFileHandle()
{
    _fileHandle = new FileHandle();
    
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
   delete _fileHandle;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    int rc =  _fileHandle->collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != 0) return -1;

    ixReadPageCounter = readPageCount;
    ixWritePageCounter = writePageCount;
    ixAppendPageCounter = appendPageCount;

    return 0;
}

