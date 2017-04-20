#include "rbfm.h"

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
    
    if(pageCount == 0  /*or all pages full*/) {
        page = malloc(PAGE_SIZE);
    }
    else {
        pageNumber = pageCount-1;
        if(fileHandle.readPage(pageNumber, page) != 0) return -1;
        recordCount = *((char *)page + PAGE_SIZE - 4);
        freeSpace = *((char *)page + PAGE_SIZE - 2);
    }
    
    
    cout << freeSpace << "   " << "\n";
    
    int test = 300;

    recordCount++;
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount)    ) = freeSpace >> 8;  
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) = freeSpace;  
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 2) = test >> 8;    
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 3) = test;    
    
    memcpy(page + freeSpace, data, 301);
    freeSpace += 300;         

    *((char *)page + PAGE_SIZE - 4) = recordCount >> 8;
    *((char *)page + PAGE_SIZE - 3) = recordCount;
    *((char *)page + PAGE_SIZE - 2) = freeSpace >> 8;
    *((char *)page + PAGE_SIZE - 1) = freeSpace;

    int append_rc;
    if(pageNumber == -1) {
        append_rc = fileHandle.appendPage(page);
        pageNumber = 0;
    }
    else {
        append_rc = fileHandle.writePage(pageNumber, page);
    }
    
    if(append_rc != 0) return -1;
    
    rid.slotNum = recordCount;
    rid.pageNum = pageNumber;
       
    return 0;
    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
        
      
    cout << rid.slotNum << "   " << rid.pageNum << "\n";
    uint16_t offset;
    uint16_t length;
    
    void *page = malloc(PAGE_SIZE);     
    fileHandle.readPage(rid.pageNum, page);
    offset  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum))     << 8;
    offset += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 1);
    length  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 2) << 8; 
    length += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 3); 
    
    
    cout << offset << "   " << length << "\n";
    
    memcpy(data, page + offset, length + 1);
       
    return 0;
    
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
