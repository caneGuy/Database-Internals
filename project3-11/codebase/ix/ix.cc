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
    int rc = _pf_manager->createFile(fileName);
    if(rc != 0) return -1;

    struct nodeHeader header;
    header.leaf = 0;
    header.pageNum = 0;
    header.freeSpace = sizeof(struct nodeHeader) + sizeof(uint16_t);
    header.left = -1;
    header.right = -1;
    
    uint16_t dummy = 1;
    
    char *data = (char*)malloc(PAGE_SIZE);
    memcpy(data, &header, sizeof(struct nodeHeader));
    memcpy(data + sizeof(struct nodeHeader), &dummy, sizeof(uint16_t));
    
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(data))
        return RBFM_APPEND_FAILED; 

    header.leaf = 1;
    header.pageNum = 1;
    header.freeSpace = sizeof(struct nodeHeader);
    header.left = -1;
    header.right = -1;
    
    memcpy(data, &header, sizeof(struct nodeHeader));
    
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
    vector<uint16_t> pageStack;
    pageStack.push_back(ROOT_PAGE);
    insertRec(ixfileHandle, attribute, key, rid, pageStack);    
    return 0;
}

RC IndexManager::insertRec(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    char* page = (char*)malloc(PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
    if (pageHeader.leaf == true) {    
        free(page);    
        return insertToLeaf(ixfileHandle, attribute, key, rid, pageStack);        
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
        free(page);
        pageStack.push_back(nextPageNum);
        return insertRec(ixfileHandle, attribute, key, rid, pageStack);
    }
}

RC IndexManager::insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* const key, RID rid, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    char* page = (char*)malloc(PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
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
        memcpy(page, &pageHeader, sizeof(struct nodeHeader));
        ixfileHandle._fileHandle->writePage(pageNum, page);
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
    
    char* page2 = (char*)malloc(PAGE_SIZE);    
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
    page2Header.pageNum = ixfileHandle._fileHandle->getNumberOfPages();
    page2Header.left = pageHeader.pageNum;
    page2Header.right = pageHeader.right;
    pageHeader.right = page2Header.pageNum;
    // write new headers
    memcpy(page, &pageHeader, sizeof(struct nodeHeader));
    memcpy(page2, &page2Header, sizeof(struct nodeHeader));
    ixfileHandle._fileHandle->writePage(pageNum, page);
    ixfileHandle._fileHandle->appendPage(page2);
    free(page);
    free(page2);
    // insert new trafficCup into parent
    pageStack.pop_back();
    insertToInterior(ixfileHandle, attribute, entry.key, pageNum, page2Header.pageNum, pageStack);
    // then try to insert again
    // this could be optimised but it is easier for now
    // and it should never cause another split on the second attemt
    return insertEntry(ixfileHandle, attribute, key, rid);      
    
}


RC IndexManager::insertToInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t oldPage, uint16_t newPage, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    char* page = (char*)malloc(PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
    uint16_t keySize = getSize(attribute, key);
    
    // normal insert
    if (pageHeader.freeSpace + keySize + sizeof(uint16_t) <= PAGE_SIZE) {        
        // find correct spot by walking down the list
        // then insert (memmove) key, page2Num
        // update header        
        uint16_t offset = sizeof(struct nodeHeader);    
        struct interiorEntry entry;
        // walk through the page until key is not smaller any more 
        // or we reached the last entry 
        uint16_t dummy;
        memcpy(&dummy, page + offset, sizeof(uint16_t));
        if (dummy != oldPage) {
            while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
                entry = nextInteriorEntry(page, attribute, offset);
                if (entry.right == oldPage)
                    break;
            }
        }
        // now offset points at the "oldPage" pointer
        // and we want to insert the key, and a pointer to "newPage" right behind this pointer
        offset += sizeof(uint16_t);
        memmove(page + offset + keySize + sizeof(uint16_t), page + offset, pageHeader.freeSpace - offset);
        memcpy(page + offset, key, keySize);
        memcpy(page + offset + keySize, &newPage, sizeof(uint16_t));
        pageHeader.freeSpace += keySize + sizeof(uint16_t);   
        memcpy(page, &pageHeader, sizeof(struct nodeHeader));
        ixfileHandle._fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    }    

    cout << "ERROR: spliting an interor Node not yet implemented" << endl;
    return -1;
    
    
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{

    print_rec(0, ROOT_PAGE, ixfileHandle, attribute);

}

void IndexManager::print_rec(uint16_t depth, uint16_t pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    
    char* page = (char*)malloc(PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
    if (pageHeader.leaf == true) {
    
        cout << string(depth * 4, ' ') << "{\"keys\": [";        
        uint16_t offset = sizeof(struct nodeHeader);       
        struct leafEntry entry;
        while (true) {
            entry = nextLeafEntry(page, attribute, offset);
            printLeafEntry(entry);
            if (pageHeader.freeSpace <= offset)
                break;
            cout << ", ";
        }        
        cout << "]}";
        
    } else {
        
        cout << string(depth * 4, ' ') << "{\"keys\": [";
        uint16_t offset = sizeof(struct nodeHeader);       
        struct interiorEntry entry;
        while (true) {
            entry = nextInteriorEntry(page, attribute, offset);
            cout << "\"";
            printKey(entry.attribute, entry.key);
            cout << "\"";
            if (pageHeader.freeSpace <= offset + sizeof(uint16_t))
                break;
            cout << ", ";
        }         
        cout << "]," << endl;
        
        cout << string(depth * 4, ' ') << " \"children\": [" << endl;        
        offset = sizeof(struct nodeHeader);
        
        /// DELETE this once we have splitting root page
        memcpy(&entry.right, page + offset, sizeof(uint16_t));
        /// DELETE end
        
        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            print_rec(depth + 1, entry.left, ixfileHandle, attribute);
            cout << "," << endl;
        }
        print_rec(depth + 1, entry.right, ixfileHandle, attribute);
        cout << string(depth * 4, ' ') << endl << "]}" << endl;
        
    }
    
}

void IndexManager::printLeafEntry(struct leafEntry entry) const {

    cout << '\"';
    printKey(entry.attribute, entry.key);
    cout << ":[(" << entry.rid.pageNum << "," << entry.rid.slotNum << ")]";
    cout << '\"';
    
}

void IndexManager::printKey(const Attribute& attribute, void* key) const {
        
    switch (attribute.type) {
        case TypeVarChar:
        {
            int size;
            memcpy(&size, key, sizeof(int));
            char string[size + 1];
            memcpy(string, key, size);
            string[size] = 0;
            cout << string;
            break;
        }
        case TypeInt:
        {
            int number;
            memcpy(&number, key, sizeof(int));
            cout << number;
            break;
        }
        case TypeReal:
        {
            float number;
            memcpy(&number, key, sizeof(float));
            cout << number;
            break;
        }
        default:
            ;
    }
    
}


struct interiorEntry IndexManager::nextInteriorEntry(char* page, Attribute attribute, uint16_t &offset) const {
    
    struct interiorEntry entry;
    entry.attribute = attribute;
    memcpy(&entry.left, page + offset, sizeof(uint16_t));    
    uint16_t size = getSize(attribute, page + offset + sizeof(uint16_t));    
    memcpy(&entry.key, page + offset + sizeof(uint16_t), size);
    memcpy(&entry.right, page + offset + sizeof(uint16_t) + size, sizeof(uint16_t));
    offset += sizeof(uint16_t) + size;
    return entry;
    
}

struct leafEntry IndexManager::nextLeafEntry(char* page, Attribute attribute, uint16_t &offset) const {
    
    struct leafEntry entry;  
    entry.attribute = attribute;
    uint16_t size = getSize(attribute, page + offset);    
    memcpy(&entry.key, page + offset, size);
    memcpy(&entry.rid, page + offset + size, sizeof(RID));
    offset += size + sizeof(RID);
    return entry;
    
}

uint16_t IndexManager::getSize(const Attribute &attribute, const void* key) const {
    
    switch (attribute.type) {
        case TypeVarChar:
            int size;
            memcpy(&size, key, sizeof(int));
            return 4 + size;
        case TypeInt:
            return 4;
        case TypeReal:
            return 4;
        default:
            return -1;
    }
    
}

RC IndexManager::isKeySmaller(const Attribute &attribute, const void* pageEntryKey, const void* key) {   

    switch (attribute.type) {
        case TypeVarChar:
        {
            int size;
            memcpy(&size, pageEntryKey, sizeof(int));
            char c_pageEntryKey[size + 1];
            memcpy(c_pageEntryKey, pageEntryKey, size);
            c_pageEntryKey[size] = 0;
            memcpy(&size, key, sizeof(int));
            char c_key[size + 1];
            memcpy(c_key, key, size);
            c_key[size] = 0;            
            return strcmp(c_key, c_pageEntryKey) >= 0;
        }
        case TypeInt:
        {
            int pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(int));
            memcpy(&searchKey, key, sizeof(int));
            return (pageKey < searchKey);
        }
        case TypeReal:
        {
            float pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(float));
            memcpy(&searchKey, key, sizeof(float));
            return (pageKey < searchKey);
        }
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

