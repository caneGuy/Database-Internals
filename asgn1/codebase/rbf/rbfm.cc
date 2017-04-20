#include "rbfm.h"
#include "cmath"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    
    void *page = NULL; 
    int pageNumber = -1;
    uint16_t freeSpace = 0;
    uint16_t recordCount = 0;
    uint16_t pageCount = fileHandle.getNumberOfPages();
    
    if (pageCount == 0  /*or all pages full*/) {
        page = malloc(PAGE_SIZE);
    } else {
        pageNumber = pageCount-1;
        if (fileHandle.readPage(pageNumber, page) != 0) return -1;
        recordCount = *((char *)page + PAGE_SIZE - 4);
        freeSpace = *((char *)page + PAGE_SIZE - 2);
    }
    
    char nullvec = ceil(recordDescriptor.size()/8.0);
    uint16_t dir = recordDescriptor.size();
    uint16_t len = 0;
    void *pointer = malloc(sizeof(uint16_t) * dir);
    
    for (int i=0; i<dir; ++i) {
        char target = *((char *)data + (char)floor(i/8));
        if(target | ~(1<<(7-i%8))) {
            len += recordDescriptor[i].length;
        }
        *((char *)pointer + i * sizeof(uint16_t)    ) = len >> 8;
        *((char *)pointer + i * sizeof(uint16_t) + 1) = len;
    }
    void* record = malloc(sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir + len);
    *((char *)record)       = dir >> 8;
    *((char *)record + 1)   = dir;
    memcpy(record + sizeof(uint16_t), data, nullvec + 1);    
    memcpy(record + sizeof(uint16_t) + nullvec, pointer, sizeof(uint16_t) * dir + 1);
    memcpy(record + sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir, data + nullvec, len + 1);
    
    uint16_t recordSize = sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir + len;
    
    cout << (int)nullvec << "   " << dir << "    " << len << endl;
    
    recordCount++;
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount)    ) = freeSpace >> 8;  
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) = freeSpace;  
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 2) = recordSize >> 8;    
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 3) = recordSize;    
    
    memcpy(page + freeSpace, record, recordSize +1);
    freeSpace += recordSize;         

    *((char *)page + PAGE_SIZE - 4) = recordCount >> 8;
    *((char *)page + PAGE_SIZE - 3) = recordCount;
    *((char *)page + PAGE_SIZE - 2) = freeSpace >> 8;
    *((char *)page + PAGE_SIZE - 1) = freeSpace;

    int append_rc;
    if (pageNumber == -1) {
        append_rc = fileHandle.appendPage(page);
        pageNumber = 0;
    } else {
        append_rc = fileHandle.writePage(pageNumber, page);
    }
    
    if (append_rc != 0) return -1;
    
    rid.slotNum = recordCount;
    rid.pageNum = pageNumber;
       
    return 0;
    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
        
    uint16_t offset;
    uint16_t length;
    uint16_t attCount;
    uint16_t base;
    
    void *page = malloc(PAGE_SIZE);     
    fileHandle.readPage(rid.pageNum, page);
    
    offset  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum))     << 8;
    offset += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 1);
    length  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 2) << 8; 
    length += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 3); 
    attCount  = *((char *)page + offset    ) << 8; 
    attCount += *((char *)page + offset + 1); 
    
    char nullBytes = (char)ceil(attCount/8.0);
    base = offset + sizeof(uint16_t) + nullBytes + sizeof(uint16_t) * attCount;
    
    int test = offset + sizeof(uint16_t) + (char)ceil(attCount/8.0) + sizeof(uint16_t) * attCount;
    cout << offset << "    " << length << "   " << test << endl;
    
    memcpy(data, page + offset + sizeof(uint16_t), nullBytes + 1);
    memcpy(data + nullBytes, page + base,  length - base + 1);
    
    return 0;
    
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
