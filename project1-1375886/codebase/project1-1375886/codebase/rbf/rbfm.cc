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
        
    const char* data_c = (char*)data;
    uint16_t count = recordDescriptor.size();
    uint16_t in_offset = ceil(recordDescriptor.size()/8.0);
    uint16_t dir_offset = sizeof(uint16_t) + ceil(recordDescriptor.size()/8.0);
    uint16_t data_offset = dir_offset + sizeof(uint16_t) * count;
    
    uint16_t max_size = data_offset;
    for (int i = 0; i < count; ++i) 
        max_size += recordDescriptor[i].length;
    
    char *record = (char*)calloc(max_size, sizeof(char));
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
    // later we append instead of overwrite in this case
    if (currPage == pageCount) {
        memset(page, 0, PAGE_SIZE); // debug only
        freeSpace = 0;
        recordCount = 0;
    }
     
    recordCount++;
    
    // write acutal record
    memcpy(page + freeSpace, record, data_offset);    
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (4 * recordCount + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &data_offset, sizeof(uint16_t));
    
    freeSpace += data_offset;
        
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    int append_rc;
    if (currPage == pageCount) {
        append_rc = fileHandle.appendPage(page);
        // cout << "new page ceated  " << currPage << "   " << append_rc << endl;
    } else {
        append_rc = fileHandle.writePage(currPage, page);
        append_rc = fileHandle.readPage(currPage, page);
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
    
    uint16_t record;
    memcpy(&record,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(uint16_t));
     
    uint16_t fieldCount;
    memcpy(&fieldCount, &page[record], sizeof(uint16_t));
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

// hex dump memory

    // cout << "page after adding record" << endl; 
    // const char* p = reinterpret_cast< const char *>( page );
    // for ( unsigned int i = 0; i < PAGE_SIZE; i++ ) {
     // std::cout << hex << int(p[i]) << " ";
    // }
     // cout << endl << "page end:" << endl;
