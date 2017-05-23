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
    return _pf_manager->createFile(fileName);;
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
    // if file is empty create a root page (as a leaf page)
    if (ixfileHandle._fileHandle->getNumberOfPages() == 0) {
        struct nodeHeader header;
        header.leaf = true;
        header.pageNum = ROOT_PAGE;
        header.freeSpace = sizeof(struct nodeHeader);
        header.left = -1;
        header.right = -1;        
        char *data = (char*)malloc(PAGE_SIZE);
        memcpy(data, &header, sizeof(struct nodeHeader));        
        ixfileHandle._fileHandle->appendPage(data);
        free(data);
    }   
    // init pageStack and make recursive call
    vector<uint16_t> pageStack;
    pageStack.push_back(ROOT_PAGE);       
    return insertRec(ixfileHandle, attribute, key, rid, pageStack); 
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
    
    // Note 2 * PAGE_SIZE
    char* page = (char*)malloc(2 * PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
    // always insert key into page
    uint16_t keySize = getSize(attribute, key);    
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
        
    // normal insert (if it still fits into one page)
    if (pageHeader.freeSpace <= PAGE_SIZE) {  
        memcpy(page, &pageHeader, sizeof(struct nodeHeader));
        ixfileHandle._fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    } 

    // try to push one into sibling?
    // lowest priority for now
    
    
    // split node (if it doesn't fit into one page)    
    char* page2 = (char*)malloc(PAGE_SIZE);    
    offset = sizeof(struct nodeHeader);
    // walk through the page until we are about half way through the page
    while (offset - (sizeof(struct nodeHeader) / 2) < PAGE_SIZE / 2) {
        entry = nextLeafEntry(page, attribute, offset);
    }
    
    /////// This code makes sure that we only split on keys that are different
    bool found = false;
    uint16_t splitKeySize = getSize(attribute, entry.key);
    char splitKey[splitKeySize];
    memcpy(splitKey, entry.key, splitKeySize);
    while (offset < pageHeader.freeSpace) {
        entry = nextLeafEntry(page, attribute, offset);
        if (memcmp(entry.key, splitKey, splitKeySize) != 0) {
            found = true;
            // cout << "found something to split on (first try)" << endl;
            // hexdump(splitKey, splitKeySize);
            // hexdump(entry.key, 4);
            break;
        }
    }
    if (not found) {
        char newSplitKey[PAGE_SIZE];
        offset = sizeof(struct nodeHeader);
        while (offset < pageHeader.freeSpace) {
            entry = nextLeafEntry(page, attribute, offset);
            if (memcmp(entry.key, splitKey, splitKeySize) == 0) {
                break;
            }
            memcpy(newSplitKey, entry.key, getSize(attribute, entry.key));
        }
        if (offset - entry.sizeOnPage == sizeof(struct nodeHeader)) {
            cerr << "ERROR: There are too many entries with the same key!" << endl;
            cerr << "That is not supported by this index class." << endl;
            free(page);
            free(page2);  
            return -1;
        } 
        offset -= entry.sizeOnPage;
        memcpy(entry.key, newSplitKey, getSize(attribute, newSplitKey));
    } else {
        offset -= entry.sizeOnPage;
        memcpy(entry.key, splitKey, splitKeySize);
    }
    /////// This code END
    
    // copy part of the page into the new page
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
    
    // if root page split it else insert new trafficCup into parent  
    RC rc;
    if (pageNum == ROOT_PAGE) {
        rc = createNewRoot(ixfileHandle, attribute, entry.key, page2Header.pageNum);
    } else {
        pageStack.pop_back();
        rc = insertToInterior(ixfileHandle, attribute, entry.key, pageNum, page2Header.pageNum, pageStack);
    }
    return rc;     
}


RC IndexManager::insertToInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t oldPage, uint16_t newPage, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    // Note 2 * PAGE_SIZE
    char* page = (char*)malloc(2 * PAGE_SIZE);
    ixfileHandle._fileHandle->readPage(pageNum, page);
    
    struct nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof(struct nodeHeader));
    
    uint16_t keySize = getSize(attribute, key);    
      
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
    
    // normal insert
    if (pageHeader.freeSpace + keySize + sizeof(uint16_t) <= PAGE_SIZE) {  
        ixfileHandle._fileHandle->writePage(pageNum, page);
        free(page);
        return 0;
    }    
    
    // split node (if it doesn't fit into one page)    
    char* page2 = (char*)malloc(PAGE_SIZE);    
    offset = sizeof(struct nodeHeader);
    // walk through the page until we are about half way through the page
    while (offset - (sizeof(struct nodeHeader) / 2) < PAGE_SIZE / 2) {
        entry = nextInteriorEntry(page, attribute, offset);
    }
    // copy part of the page into the new page
    // begin with pointer after entry.key
    memcpy(page2 + sizeof(struct nodeHeader), page + offset, pageHeader.freeSpace - offset); 
    // set up headers
    struct nodeHeader page2Header;
    page2Header.freeSpace = sizeof(struct nodeHeader) + pageHeader.freeSpace - offset;
    // page ends now with the pointer that was right before enty.key
    pageHeader.freeSpace = offset - getSize(attribute, entry.key);
    page2Header.leaf = false;
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
        
    // if root page split it else insert new trafficCup into parent  
    RC rc;
    if (pageNum == ROOT_PAGE) {
        rc = createNewRoot(ixfileHandle, attribute, entry.key, page2Header.pageNum);
    } else {
        pageStack.pop_back();
        rc = insertToInterior(ixfileHandle, attribute, entry.key, pageNum, page2Header.pageNum, pageStack);
    }
    return rc;          
}

RC IndexManager::createNewRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t page2Num) {
    
    char* newRoot = (char*)malloc(PAGE_SIZE);
    char* oldRoot = (char*)malloc(PAGE_SIZE);
    char* page2 = (char*)malloc(PAGE_SIZE);
    
    ixfileHandle._fileHandle->readPage(ROOT_PAGE, oldRoot);
    ixfileHandle._fileHandle->readPage(page2Num, page2);
    
    struct nodeHeader oldRootHeader;
    memcpy(&oldRootHeader, oldRoot, sizeof(struct nodeHeader));
    struct nodeHeader page2Header;
    memcpy(&page2Header, page2, sizeof(struct nodeHeader));
    
    oldRootHeader.pageNum = ixfileHandle._fileHandle->getNumberOfPages();
    page2Header.left = oldRootHeader.pageNum;
    // cout << "new pageNum: " << oldRootHeader.pageNum << endl;
    
    struct nodeHeader newRootHeader;
    
    newRootHeader.leaf = 0;
    newRootHeader.pageNum = ROOT_PAGE;
    newRootHeader.left = -1;
    newRootHeader.right = -1;
    
    uint16_t keySize = getSize(attribute, key);
    uint16_t offset = sizeof(struct nodeHeader);
    memcpy(newRoot + offset, &oldRootHeader.pageNum, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memcpy(newRoot + offset, key, keySize);
    offset += keySize;
    memcpy(newRoot + offset, &page2Num, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    newRootHeader.freeSpace = offset;
    
    memcpy(newRoot, &newRootHeader, sizeof(struct nodeHeader));
    memcpy(oldRoot, &oldRootHeader, sizeof(struct nodeHeader));
    memcpy(page2,   &page2Header,   sizeof(struct nodeHeader));
    
    ixfileHandle._fileHandle->writePage(ROOT_PAGE, newRoot);
    ixfileHandle._fileHandle->writePage(page2Num, page2);
    ixfileHandle._fileHandle->appendPage(oldRoot);
    free(newRoot);
    free(oldRoot);
    free(page2);
    
    return 0;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    
        
    // char* page = (char*)malloc(PAGE_SIZE);
    // ixfileHandle._fileHandle->readPage(0, page);
    
  // unsigned char *buf = (unsigned char*)page;
  // int i, j;
  // for (i=0; i<PAGE_SIZE; i+=16) {
    // printf("%06x: ", i);
    // for (j=0; j<16; j++) 
      // if (i+j < PAGE_SIZE)
        // printf("%02x ", buf[i+j]);
      // else
        // printf("   ");
    // printf(" ");
    // for (j=0; j<16; j++) 
      // if (i+j < PAGE_SIZE)
        // printf("%c", isprint(buf[i+j]) ? buf[i+j] : '.');
    // printf("\n");
  // }   
    
  // free(page);
    
    print_rec(0, ROOT_PAGE, ixfileHandle, attribute);
    cout << endl;
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
        cout << endl << string(depth * 4, ' ') << "]}";
        
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
            // cout << endl << size << endl;
            // cout << endl;
            // hexdump(key, size);
            char string[size + 1];
            memcpy(string, (char*)key + 4, size);
            size = 10;
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
    entry.sizeOnPage = size + sizeof(RID);
    offset += entry.sizeOnPage;
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

void IndexManager::hexdump(const void *ptr, int buflen) {
  unsigned char *buf = (unsigned char*)ptr;
  int i, j;
  for (i=0; i<buflen; i+=16) {
    printf("%06x: ", i);
    for (j=0; j<16; j++) 
      if (i+j < buflen)
        printf("%02x ", buf[i+j]);
      else
        printf("   ");
    printf(" ");
    for (j=0; j<16; j++) 
      if (i+j < buflen)
        printf("%c", isprint(buf[i+j]) ? buf[i+j] : '.');
    printf("\n");
  }
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
