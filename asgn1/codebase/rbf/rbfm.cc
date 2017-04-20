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
    }
      
    recordCount = *((char *)page + PAGE_SIZE - 4) + 1;
    freeSpace = *((char *)page + PAGE_SIZE - 2);

    *((char *)page + PAGE_SIZE - 4 - (4*recordCount)) = freeSpace;  
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 2) = sizeof(8);    

    memcpy(page + freeSpace, data, sizeof(8));
    freeSpace += sizeof(8);         

    *((char *)page + PAGE_SIZE - 4) = recordCount;
    *((char *)page + PAGE_SIZE - 2) = freeSpace;

    int append_rc;
    if(pageNumber == -1)
        append_rc = fileHandle.appendPage(page);
    else 
        append_rc = fileHandle.writePage(pageNumber, page);
    
    
    if(append_rc != 0) return -1;
       
    return 0;
    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
        
    void *page = NULL;     
    fileHandle.readPage(rid.pageNum, page);
    uint16_t offset = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum)); 
    uint16_t length = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 2); 
    memcpy(data, page + offset, length);
       
    return 0;
    
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
