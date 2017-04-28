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
    free(_rbf_manager);
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
 
    char *record = (char*)calloc(PAGE_SIZE, sizeof(char));
    
    uint16_t record_length = makeRecord(recordDescriptor, data, record);
     
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char)); 
    uint16_t pageCount = fileHandle.getNumberOfPages(); 
    uint16_t currPage = (pageCount>0) ? pageCount-1 : 0;
    uint16_t freeSpace;
    uint16_t recordCount;  
    
    char first = 1;
    while (currPage < pageCount) {        
        if (fileHandle.readPage(currPage, page) != 0) return -1;
        
        memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
        memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  

        if (record_length + 4 <= PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
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
    // later we append instead of overwrite in this case
    if (currPage == pageCount) {
        memset(page, 0, PAGE_SIZE); // debug only
        freeSpace = 0;
        recordCount = 0;
    }
     
    recordCount++;
    
    // write acutal record
    memcpy(page + freeSpace, record, record_length);    
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (4 * recordCount + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &record_length, sizeof(uint16_t));
    
    freeSpace += record_length;
        
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    int append_rc;
    if (currPage == pageCount) {
        append_rc = fileHandle.appendPage(page);
        // cout << "new page ceated  " << currPage << "   " << append_rc << endl;
    } else {
        append_rc = fileHandle.writePage(currPage, page);
    }
    if (append_rc != 0) return -1;
    
    rid.slotNum = recordCount;
    rid.pageNum = currPage;
    free(record);
    free(page);
    
    return 0;
    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));      
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) return -1;   

    uint16_t count;
    memcpy(&count, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    if(rid.slotNum > count) return -1;
    
    int16_t record;
    memcpy(&record,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(int16_t));
    
    if (record < 0) {
        cout << "This record was moved to a different page!" << endl;
        cout << "Old RID: " << rid.pageNum << "." << rid.slotNum << endl;
        
        uint16_t pageNum;
        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));
        
        RID new_rid;
        new_rid.pageNum = pageNum;
        new_rid.slotNum = record * (-1);       
        cout << "New RID: " << new_rid.pageNum << "." << new_rid.slotNum << endl;
 
        return readRecord(fileHandle, recordDescriptor, new_rid, data);
    }
     
    uint16_t fieldCount;
    memcpy(&fieldCount, &page[record], sizeof(int16_t));
    if (fieldCount != recordDescriptor.size()) return -1;

    uint16_t nullvec = ceil(fieldCount / 8.0);
    memcpy(data, &page[record + sizeof(uint16_t)], nullvec);
    
    uint16_t directory = record + sizeof(uint16_t) + nullvec;
    
    uint16_t offset = sizeof(uint16_t) + nullvec + fieldCount * sizeof(uint16_t);
    uint16_t prev_offset = offset;
    char *data_c = (char*)data+nullvec;
    
    for(uint16_t i = 0; i < fieldCount; ++i) {
        char target = *((char*)data + (char)(i/8));
        if (!(target & (1<<(7-i%8)))) {
            memcpy(&offset, &page[directory + i * sizeof(uint16_t)], sizeof(uint16_t));
            if (recordDescriptor[i].type == TypeVarChar) {
                int attlen = offset - prev_offset;
                memcpy(&data_c[0], &attlen, sizeof(int));
                memcpy(&data_c[4], &page[record + prev_offset], attlen);
                data_c += (4 + attlen);                
            } else {
                memcpy(&data_c[0], &page[record + prev_offset], sizeof(int));
                data_c += sizeof(int); 
            }
        }
        prev_offset = offset;
    }
    free(page);

    return 0;
    
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));      
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) return -1;   

        
    // cout << "page after adding record" << endl; 
    // const char* p = reinterpret_cast< const char *>( page );
    // for ( unsigned int i = 0; i < PAGE_SIZE; i++ ) {
     // std::cout <<  int(p[i]) << " ";
    // }
     // cout << endl << "page end:" << endl;
    
    uint16_t record_count;
    uint16_t free_space;
    memcpy(&record_count, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    memcpy(&free_space, &page[PAGE_SIZE - 2], sizeof(uint16_t));  
    if(rid.slotNum > record_count) return -1;
    
    uint16_t record_offset;
    memcpy(&record_offset,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(uint16_t));
    uint16_t record_length;
    memcpy(&record_length,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));
    
    // move data
    memmove(&page[record_offset], &page[record_offset + record_length], free_space - record_offset - record_length);
    
    // update free space
    free_space -= record_length;
    memcpy(&page[PAGE_SIZE - 2], &free_space, sizeof(uint16_t));  
    
    // update dir entries
    for (int i = rid.slotNum + 1; i <= record_count; ++i) {
        
        memcpy(&record_offset,  &page[PAGE_SIZE - 4 - (4 * i)], sizeof(uint16_t));
        record_offset -= record_length;       
        memcpy(&page[PAGE_SIZE - 4 - (4 * i)], &record_offset, sizeof(uint16_t)); 
        
    }    
    
    int32_t dummy = -1;
    memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &dummy, sizeof(int32_t));
    
    
    // cout << "page after adding record" << endl; 
    // p = reinterpret_cast< const char *>( page );
    // for ( unsigned int i = 0; i < PAGE_SIZE; i++ ) {
     // std::cout << int(p[i]) << " ";
    // }
     // cout << endl << "page end:" << endl;

    if (fileHandle.writePage(rid.pageNum, page) != 0) return -1;
    
    free(page);
    return 0;
    
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    
    deleteRecord(fileHandle, recordDescriptor, rid);
    
    if (tryInsert(fileHandle, recordDescriptor, data, rid) == 0) return 0;    
    
    RID new_rid;
    insertRecord(fileHandle, recordDescriptor, data, new_rid);
    
    // cout << new_rid.pageNum << '.' << new_rid.slotNum << endl;
        
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char)); 
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) return -1;     
    
     int16_t new_slotNum = new_rid.slotNum * (-1);
    uint16_t new_pageNum = new_rid.pageNum;
    memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &new_slotNum, sizeof( int16_t));
    memcpy(&page[PAGE_SIZE - 2 - (4 * rid.slotNum)], &new_pageNum, sizeof(uint16_t));
    
    if (fileHandle.writePage(rid.pageNum, page) != -1) return -1;
    free(page);
    
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
                //cout << "atlen: " << attlen << endl;
                char content[attlen + 1];
                memcpy(content, &data_c[offset + sizeof(int)], attlen );
                content[attlen] = 0;
                /*for(int pos = 0; pos < attlen; ++pos) {
                    cout << &data_c[offset + sizeof(int) + pos];
                }*/
                cout << recordDescriptor[i].name << ": " << content << "\t";
                offset += (4 + attlen);
                //cout << &content << " " << &data_c << " " << offset << endl;
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



RC RecordBasedFileManager::tryInsert(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
   
    char *record = (char*)calloc(1000, sizeof(char));
    
    uint16_t record_length = makeRecord(recordDescriptor, data, record);
     
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char));       
    if (fileHandle.readPage(rid.pageNum, page) != 0) return -1;
    
    uint16_t recordCount, freeSpace;   
    memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  

    if (record_length + 4 > PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
        
        cout << "doesn't fit on this page any more" << endl;
        free(record);
        free(page);
        return -1;
    }
    
    cout << "updated record fit on same page" << endl;
    // cout << "free space is: " << freeSpace << endl;
    // cout << "record_length is: " << record_length << endl;
    
    // write acutal record
    memcpy(page + freeSpace, record, record_length);    
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (4 * rid.slotNum + 4);
    
    
    cout << "page_dir is: " << page_dir << endl;
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &record_length, sizeof(uint16_t));
    
    freeSpace += record_length;

    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    if (fileHandle.writePage(rid.pageNum, page) != 0) return -1;
    
    free(record);
    free(page);
    
    return 0;
    
}

uint16_t RecordBasedFileManager::makeRecord(const vector<Attribute> &recordDescriptor, const void *data, char *record) {
        
    const char* data_c = (char*)data;
    uint16_t count = recordDescriptor.size();
    uint16_t in_offset = ceil(recordDescriptor.size()/8.0);
    uint16_t dir_offset = sizeof(uint16_t) + ceil(recordDescriptor.size()/8.0);
    uint16_t data_offset = dir_offset + sizeof(uint16_t) * count;
    
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
            
        } else {
           
            // write "dir entry"
            memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));
            
        }
        
        dir_offset += sizeof(uint16_t);
        
    }     
    
    return data_offset;
    
}


// hex dump memory

    // cout << "page after adding record" << endl; 
    // const char* p = reinterpret_cast< const char *>( page );
    // for ( unsigned int i = 0; i < PAGE_SIZE; i++ ) {
     // std::cout << hex << int(p[i]) << " ";
    // }
     // cout << endl << "page end:" << endl;
