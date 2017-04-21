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
        
    const char* data_c = (char*)data;
    uint16_t count = recordDescriptor.size();
    uint16_t in_offset = ceil(recordDescriptor.size()/8.0);
    uint16_t dir_offset = sizeof(uint16_t) + ceil(recordDescriptor.size()/8.0);
    uint16_t data_offset = dir_offset + sizeof(uint16_t) * count;
    
    uint16_t max_size = data_offset;
    for (int i = 0; i < count; ++i) 
        max_size += recordDescriptor[i].length;
    
    char *record = (char*)malloc(max_size);
    memcpy(record, &count, sizeof(uint16_t));
    memcpy(record + sizeof(uint16_t), &data_c[0], in_offset);
    
    for (int i = 0; i < count; ++i) {
        
        char target = *(data_c + (char)(i/8));
        if (!(target & (1<<(7-i%8)))) {
            
            if (recordDescriptor[i].type == TypeVarChar) {
                int attlen;
                // read length
                memcpy(&attlen, &data_c[in_offset], sizeof(int));
                // wirte data
                memcpy(record + data_offset, &data_c[in_offset + 4], attlen);
                data_offset += attlen;
                in_offset += (4 + attlen);
                // write "dir entry"
                memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));           
            } else {
                if (recordDescriptor[i].type == TypeInt) {
                    memcpy(record + data_offset, &data_c[in_offset], sizeof(int));
                    in_offset += sizeof(int);
                    data_offset += sizeof(int); 
                    // write "dir entry"
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));  
                } 
                if (recordDescriptor[i].type == TypeReal) {
                    memcpy(record + data_offset, &data_c[in_offset], sizeof(float));
                    in_offset += sizeof(float);
                    data_offset += sizeof(float); 
                    // write "dir entry"
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t)); 
                }
            }
            dir_offset += sizeof(uint16_t);
            
        }
        
    }
    
    // Record is now complete (length is stored in data_offset)
        
    // char nullvec = ceil(recordDescriptor.size()/8.0);
    // uint16_t dir = recordDescriptor.size();
    // uint16_t len = 0;
    // void *pointer = malloc(sizeof(uint16_t) * dir);
    
    // for (int i=0; i<dir; ++i) {
        // char target = *((char *)data + (char)floor(i/8));
        // if(target | ~(1<<(7-i%8))) {
            // len += recordDescriptor[i].length;
        // }
        // *((char *)pointer + i * sizeof(uint16_t)    ) = len >> 8;
        // *((char *)pointer + i * sizeof(uint16_t) + 1) = len;
    // }
    // void* record = malloc(sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir + len);
    // *((char *)record)       = dir >> 8;
    // *((char *)record + 1)   = dir;
    // memcpy(record + sizeof(uint16_t), data, nullvec + 1);    
    // memcpy(record + sizeof(uint16_t) + nullvec, pointer, sizeof(uint16_t) * dir + 1);
    // memcpy(record + sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir, data + nullvec, len + 1);
    
    // uint16_t recordSize = sizeof(uint16_t) + nullvec + sizeof(uint16_t) * dir + len;    
    
     
    char *page = (char*)malloc(PAGE_SIZE); 
    uint16_t pageCount = fileHandle.getNumberOfPages(); 
    uint16_t currPage = (pageCount>0) ? pageCount-1 : 0;
    uint16_t freeSpace;
    uint16_t recordCount;
    
    char first = 1;
    while (currPage < pageCount) {        
        if (fileHandle.readPage(currPage, page) != 0) return -1;
        
        memcpy(&pageCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
        memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  
        
        cout << pageCount << "   " << freeSpace << endl;

        if (data_offset + 4 <= PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
            break;
        }
        
        if (!first) {
            ++currPage;
        } else {
            currPage = 0;
            first = 0;
        }
    }    
    
    // not enough space on any pages
    // later we append inseat of overwrite in this case
    if (currPage == pageCount) {
        freeSpace = 0;
        recordCount = 0;
    }
     
    recordCount++;
    
    // write acutal record
    memcpy(page + freeSpace, record, data_offset);
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (freeSpace + 4 * recordCount + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &data_offset, sizeof(uint16_t));
    
    freeSpace += data_offset;
        
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));   
    

    // recordCount++;
    // cout << "freespace: " << freeSpace << endl;
    // *((char *)page + PAGE_SIZE - 4 - (4*recordCount)    ) = freeSpace >> 8;  
    // cout << "offset b4: " << (int) *((char *)page + PAGE_SIZE - 4 - (4*recordCount)) << endl;
    // *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) = freeSpace;  
    // cout << "offset after: " << (int) *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 1) << endl;
    // *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 2) = recordSize >> 8;    
    // *((char *)page + PAGE_SIZE - 4 - (4*recordCount) + 3) = recordSize;    
    
    // memcpy(page + freeSpace, record, recordSize +1);
    // cout << "record size: " << recordSize << endl;
    // freeSpace += recordSize;         

    // *((char *)page + PAGE_SIZE - 4) = recordCount >> 8;
    // *((char *)page + PAGE_SIZE - 3) = recordCount;
    // *((char *)page + PAGE_SIZE - 2) = freeSpace >> 8;
    // *((char *)page + PAGE_SIZE - 1) = freeSpace;
    
    // cout << "freeSpace after 1 : " << (int) *((char *)page + PAGE_SIZE - 2) << endl;
    // cout << "freeSpace after 2: " << *((char *)page + PAGE_SIZE - 1) << endl;

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
        
    // uint16_t offset;
    // uint16_t length;
    // uint16_t attCount;
    // uint16_t base;
    
    // void *page = malloc(PAGE_SIZE);     
    // fileHandle.readPage(rid.pageNum, page);
    
    // offset  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum))     << 8;
    // cout << "Offset: " << offset << endl;
    // offset += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 1);
    // cout << "Offset: " << (int)offset << endl;
    // length  = *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 2) << 8; 
    // length += *((char *)page + PAGE_SIZE - 4 - (4*rid.slotNum) + 3); 
    // attCount  = *((char *)page + offset    ) << 8; 
    // attCount += *((char *)page + offset + 1); 
    
    // char nullBytes = (char)ceil(attCount/8.0);
    // base = offset + sizeof(uint16_t) + nullBytes + sizeof(uint16_t) * attCount;
    
    // int test = offset + sizeof(uint16_t) + (char)ceil(attCount/8.0) + sizeof(uint16_t) * attCount;
    // cout << "pageNum: " << rid.pageNum <<  "  slotNum: " << rid.slotNum << endl;
    // cout << "offset: " << offset << " length: " << length << " attCount: " << attCount << endl;
    
    // memcpy(data, page + offset + sizeof(uint16_t), nullBytes + 1);
    // memcpy(data + nullBytes, page + base,  length - base + 1);
    
    return 0;
    
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    
    uint16_t count = recordDescriptor.size();
    uint16_t offset = ceil(recordDescriptor.size()/8.0);
    const char* data_c = (char*)data;
    
    for (int i = 0; i < count; ++i) {
        
        char target = *(data_c + (char)(i/8));
        if (!(target & (1<<(7-i%8)))) {
            
            if (recordDescriptor[i].type == TypeVarChar) {
                int attlen;
                memcpy(&attlen, &data_c[offset], sizeof(int));
                char content[attlen + 1];
                memcpy(content, &data_c[offset + 4], attlen + 1);
                content[attlen] = 0;
                cout << recordDescriptor[i].name << ": " << content << "\t";
                offset += (4 + attlen);                
            } else {
                if (recordDescriptor[i].type == TypeInt) {
                    int num;
                    memcpy(&num, &data_c[offset], sizeof(int));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(int); 
                } 
                if (recordDescriptor[i].type == TypeReal) {
                    float num;
                    memcpy(&num, &data_c[offset], sizeof(float));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(float); 
                }
            }
            
        } else {
            
            cout << recordDescriptor[i].name << ": NULL\t";
            
        }
        
    }
    
    cout << endl; 
    
    return 0;
}
