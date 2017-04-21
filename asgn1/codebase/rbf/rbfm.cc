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
    
     
    void *page = malloc(PAGE_SIZE); 
    uint16_t pageCount = fileHandle.getNumberOfPages(); 
    uint16_t currPage = (pageCount>0) ? pageCount-1 : pageCount;
    uint16_t freeSpace = 0;
    uint16_t recordCount = 0;
    
    char checked = 0;
    while (currPage < pageCount) {
        
        if (fileHandle.readPage(currPage, page) != 0) return -1;
        
        recordCount  = *((char *)page + PAGE_SIZE - 4) << 8;
        recordCount += *((char *)page + PAGE_SIZE - 3)     ;
        freeSpace    = *((char *)page + PAGE_SIZE - 2) << 8;
        freeSpace   += *((char *)page + PAGE_SIZE - 1)     ;   

        if (recordSize + 4 <= PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
            break;
        }
        
        if (checked) {
            ++currPage;
        } else {
            currPage = 0;
            checked = 1;
        }
    }
    

    recordCount++;
    cout << "freespace: " << freeSpace << endl;
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount)    ) = freeSpace >> 8;  
    cout << "offset b4: " << (int) *((char *)page + PAGE_SIZE - 4 - (4*recordCount)) << endl;
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) = freeSpace;  
    cout << "offset after: " << (int) *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) << endl;
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 2) = recordSize >> 8;    
    *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 3) = recordSize;    
    
    memcpy(page + freeSpace, record, recordSize +1);
    cout << "record size: " << recordSize << endl;
    freeSpace += recordSize;         

    *((char *)page + PAGE_SIZE - 4) = recordCount >> 8;
    *((char *)page + PAGE_SIZE - 3) = recordCount;
    *((char *)page + PAGE_SIZE - 2) = freeSpace >> 8;
    *((char *)page + PAGE_SIZE - 1) = freeSpace;
    
    cout << "freeSpace after 1 : " << (int) *((char *)page + PAGE_SIZE - 2) << endl;
    cout << "freeSpace after 2: " << *((char *)page + PAGE_SIZE - 1) << endl;

    int append_rc;
    if (currPage == pageCount) {
        append_rc = fileHandle.appendPage(page);
        cout << "new page ceated  " << currPage << endl;
    } else {
        append_rc = fileHandle.writePage(currPage, page);
    }
    
    if (append_rc != 0) return -1;
    
    rid.slotNum = recordCount;
    rid.pageNum = currPage;
       
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
    cout << "Offset: " << offset << endl;
    offset += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 1);
    cout << "Offset: " << (int)offset << endl;
    length  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 2) << 8; 
    length += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 3); 
    attCount  = *((char *)page + offset    ) << 8; 
    attCount += *((char *)page + offset + 1); 
    
    char nullBytes = (char)ceil(attCount/8.0);
    base = offset + sizeof(uint16_t) + nullBytes + sizeof(uint16_t) * attCount;
    
    int test = offset + sizeof(uint16_t) + (char)ceil(attCount/8.0) + sizeof(uint16_t) * attCount;
    cout << "pageNum: " << rid.pageNum <<  "  slotNum: " << rid.slotNum << endl;
    cout << "offset: " << offset << " length: " << length << " attCount: " << attCount << endl;
    
    memcpy(data, page + offset + sizeof(uint16_t), nullBytes + 1);
    memcpy(data + nullBytes, page + base,  length - base + 1);
    
    return 0;
    
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    
    uint16_t count = recordDescriptor.size();
    uint16_t len = ceil(recordDescriptor.size()/8.0);
    const char* data_c = (char*)data;
    
    for (int i = 0; i < count; ++i) {
        
        char target = *(data_c + (char)(i/8));
        if (!(target & (1<<(7-i%8)))) {
            
            if (recordDescriptor[i].type == TypeVarChar) {
                int attlen;
                memcpy(&attlen, &data_c[len], sizeof(int));
                char content[attlen + 1];
                memcpy(content, &data_c[len + 4], attlen + 1);
                content[attlen] = 0;
                cout << recordDescriptor[i].name << ": " << content << "\t";
                len += (4 + attlen);                
            } else {
                if (recordDescriptor[i].type == TypeInt) {
                    int num;
                    memcpy(&num, &data_c[len], sizeof(int));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    len += sizeof(int); 
                } 
                if (recordDescriptor[i].type == TypeReal) {
                    float num;
                    memcpy(&num, &data_c[len], sizeof(float));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    len += sizeof(float); 
                }
            }
            
        }
        
    }
    
    cout << endl; 
    
    return 0;
}
