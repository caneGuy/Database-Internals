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


insertRec(uint16_t pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    free(page);
    
    if (pageHeader.leaf == true) {        
        insertToLeaf(pageNum, attribute, key, rid);        
    } else {        
        uint16_t pageNum = findOnInteriorPage(page, pageHeader.freeSpace, attribute, key);
        insertRec(pageNum, ixfileHandle, attribute, key, rid);
    }
}

// findOnLeafPage should be the same with sizeof(RID) (and returns an RID)
uint16_t findOnInteriorPage(page, freeSpace, attribute, key) {

    uint16_t leftPointer = sizeof(struct nodeHeader);
    uint16_t currTrafficCup = leftPointer + sizeof(uint16_t);
    
    while (currTrafficCup < freeSpace) {
        // if currTrafficCup >= key
        if (compareNext(page, currTrafficCup, attribute, key) == 0)
            break;
        leftPointer = currTrafficCup;
        currTrafficCup = currTrafficCup + sizeof(uint16_t);
    }
        
    uint16_t pageNum;
    memcpy(&pageNum, page + leftPointer, sizeof(uint16_t));
    
    return pageNum;
    
}

RC compareNext(page, &offset, attribute, key) {   

    switch (attribute.type) {
        case TypeVarChar:
            int size;
            char[attribute.length + 1] value;
            memcpy(&size, page + offset, sizeof(int));
            memcpy(value, page + offset + sizeof(int), size);
            offset += 4 + size;
            c_value[size] = NULL;
            return (strcmp((char*) key, c_value) >= 0);
        case TypeVarInt:
            int value;
            memcpy(&i_value, page + offset, sizeof(int));
            offset += 4;
            return (value < key);
        case TypeVarFloat:
            float value;
            memcpy(&f_value, page + offset, sizeof(float));
            offset += 4;
            return (value < key);
        default:
            return -1;
    }
    
}

RC insertToLeaf(pageNum, attribute, key, rid){
    
    char* page = malloc(PAGE_SIZE);
    ixfileHandle->_fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&header, page, sizeof(struct header));
    
    uint16_t size = getSize(attribute, key);
    
    // normal insert
    if (pageHeader.freeSpace + getSize(attribute, key) + sizeof(rid) <= PAGE_SIZE) {
        // first find the correct spot!
        // then memmove() and do something like this
        memcpy(page + pageHeader.freeSpace, key, size);
        memcpy(page + pageHeader.freeSpace + size, rid, sizeof(rid));
        pageHeader.freeSpace += size + sizeof(rid);
        memcpy(page, &pageHeader, sizeof(struct nodeHeader);
        ixfileHandle->_fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    } 
    
    // try to push one into sibling?
    // lowest priority for now
    
    
    // split node      
    char* page2 = malloc(PAGE_SIZE);
    char[attribute.length] trafficCup;  
    
    // set up new header, and update the other one
    // walk down the list until approximatly in the middle
    // copy half of the data into the new page (don't forget to update freeSpace)
    // last entry on left page will be trafficCup
    // overwrite page, append page2 (remember "page2Num")
    // call:    
    insertToInterior(pageHeader.parent, attribute, trafficCup, page2Num);
       
    
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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

