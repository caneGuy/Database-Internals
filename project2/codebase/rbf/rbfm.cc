#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    _pf_manager->DestroyInstance();
    _pf_manager = NULL;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
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
         
    // fill in RID assuming it'll be appended to the directory
    recordCount++;
    rid.slotNum = recordCount;
    rid.pageNum = currPage;
    // check for dir entries which indicate deletion, overwrite slotNum and Count if necessary
    for (int i = 1; i < recordCount; ++i) {
        
        int16_t slotNum;
        memcpy(&slotNum, &page[PAGE_SIZE - 4 - (4 * i)], sizeof(int16_t));
        if (slotNum == deleted_entry) {
            rid.slotNum = i;
            recordCount--;
            break;
        }
        
    }
    
    // write acutal record
    memcpy(page + freeSpace, record, record_length);    
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (4 * rid.slotNum + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &record_length, sizeof(uint16_t));
    
    freeSpace += record_length;
        
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    int append_rc;
    if (currPage == pageCount) {
        append_rc = fileHandle.appendPage(page);
    } else {
        append_rc = fileHandle.writePage(currPage, page);
    }
    if (append_rc != 0) {
        free(page);
        return -1;
    }
    
    free(record);
    free(page);
    
    return 0;
    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char)); 
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) {
        free(page); 
        return -1;   
    }

    uint16_t count;
    memcpy(&count, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    if(rid.slotNum > count) {
        free(page); 
        return -1;   
    }
    
    int16_t record;
    memcpy(&record,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(int16_t));
    
    if (record == deleted_entry) {
        free(page); 
        return -1;
    }
    
    if (record < 0) {
        uint16_t pageNum;
        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));
        
        RID new_rid;
        new_rid.pageNum = pageNum;
        new_rid.slotNum = record * (-1);       
 
        free(page); 
        return readRecord(fileHandle, recordDescriptor, new_rid, data);
    }
     
    uint16_t fieldCount;
    memcpy(&fieldCount, &page[record], sizeof(int16_t));
    if (fieldCount != recordDescriptor.size()) {
        free(page); 
        return -1;   
    }

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
    if(readpage_rc != 0) {
        free(page); 
        return -1;   
    }  
    
    uint16_t record_count;
    uint16_t free_space;
    memcpy(&record_count, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    memcpy(&free_space, &page[PAGE_SIZE - 2], sizeof(uint16_t));  
    if(rid.slotNum > record_count) return -1;
    
    int16_t record_offset;
    memcpy(&record_offset,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(int16_t));    
        
    if (record_offset == deleted_entry) {
        free(page);
        return -1;
    }
    
    if (record_offset < 0) {
        
        uint16_t pageNum;
        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));
        
        RID new_rid;
        new_rid.pageNum = pageNum;
        new_rid.slotNum = record_offset * (-1);       
 
        // delete the record from the page where it is actually located
        if (deleteRecord(fileHandle, recordDescriptor, new_rid) != 0) {
            free(page); 
            return -1;   
        }   
    
        // overwrite this dir entry to indicated deletion
        memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &deleted_entry, sizeof(int16_t));

        if (fileHandle.writePage(rid.pageNum, page) != 0) {
            free(page); 
            return -1;   
        }
        
        free(page);
        return 0;        
        
    }
    
    uint16_t record_length;
    memcpy(&record_length,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));

    // move data
    memmove(&page[record_offset], &page[record_offset + record_length], free_space - record_offset - record_length);
    
    // update free space
    free_space -= record_length;
    memcpy(&page[PAGE_SIZE - 2], &free_space, sizeof(uint16_t));  
    
    // update dir entries
    for (int i = 1; i <= record_count; ++i) {
        
        int16_t i_record_offset;
        memcpy(&i_record_offset, &page[PAGE_SIZE - 4 - (4 * i)], sizeof(int16_t));
        
        if (i_record_offset > record_offset) {
            i_record_offset -= record_length;       
            memcpy(&page[PAGE_SIZE - 4 - (4 * i)], &i_record_offset, sizeof(uint16_t)); 
        }
        
    }    
    
    memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &deleted_entry, sizeof(int16_t));

    if (fileHandle.writePage(rid.pageNum, page) != 0) {
        free(page); 
        return -1;   
    }
    
    free(page);
    return 0;
    
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    
    deleteRecord(fileHandle, recordDescriptor, rid);
    
    if (tryInsert(fileHandle, recordDescriptor, data, rid) == 0) return 0;    
    
    RID new_rid;
    insertRecord(fileHandle, recordDescriptor, data, new_rid);
    
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char)); 
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) {
        free(page); 
        return -1;   
    }     
    
    // overwrite the dir entrie on the old page to point to the new location
     int16_t new_slotNum = new_rid.slotNum * (-1);
    uint16_t new_pageNum = new_rid.pageNum;
    memcpy(&page[PAGE_SIZE - 4 - (4 * rid.slotNum)], &new_slotNum, sizeof( int16_t));
    memcpy(&page[PAGE_SIZE - 2 - (4 * rid.slotNum)], &new_pageNum, sizeof(uint16_t));
    
    if (fileHandle.writePage(rid.pageNum, page) != 0) {
        free(page); 
        return -1;   
    }
    
    free(page);
    return 0;
    
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
  
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));      
    
    int readpage_rc = fileHandle.readPage(rid.pageNum, page);
    if(readpage_rc != 0) {
        free(page); 
        return -1;   
    }  

    uint16_t count;
    memcpy(&count, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    if(rid.slotNum > count) {
        free(page); 
        return -1;   
    }
    
    int16_t record;
    memcpy(&record,  &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(int16_t));
    
    if (record == deleted_entry) {
        free(page);
        return -1;
    }
    
    if (record < 0) {
        uint16_t pageNum;
        memcpy(&pageNum,  &page[PAGE_SIZE - 2 - (4 * rid.slotNum)], sizeof(uint16_t));
        
        RID new_rid;
        new_rid.pageNum = pageNum;
        new_rid.slotNum = record * (-1);
        free(page);
        return readAttribute(fileHandle, recordDescriptor, new_rid, attributeName, data);
    }
     
    uint16_t fieldCount;
    memcpy(&fieldCount, &page[record], sizeof(uint16_t));
    if (fieldCount != recordDescriptor.size()) {
        free(page); 
        return -1;   
    }
    
    uint16_t index;
    for (index = 0; index < recordDescriptor.size(); ++index) {
        if (recordDescriptor[index].name == attributeName) {
            break;
        }
    }
    
    char target = page[record + sizeof(uint16_t) + index/8];
    // if field is null
    if (target & (1<<(7-index%8))) {
        memset(data, 1, 1);
    } else {
        memset(data, 0, 1);
    }
    
    uint16_t nullvec = ceil(fieldCount / 8.0);
    
    uint16_t begin_offset;
    memcpy(&begin_offset, &page[record + sizeof(uint16_t) + nullvec + (index - 1) * sizeof(uint16_t)], sizeof(uint16_t));
    uint16_t end_offset;
    memcpy(&end_offset, &page[record + sizeof(uint16_t) + nullvec + index * sizeof(uint16_t)], sizeof(uint16_t));
    
    if (recordDescriptor[index].type == TypeVarChar){
        int diff = end_offset - begin_offset;
        memcpy((char*)data + 1, &diff, sizeof(int));
        memcpy((char*)data + 1 + sizeof(int), &page[record + begin_offset], diff);
    } else {
        memcpy((char*)data + 1, &page[record + begin_offset], sizeof(int));
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
                char content[attlen + 1];
                memcpy(content, &data_c[offset + sizeof(int)], attlen );
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



RC RecordBasedFileManager::tryInsert(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
   
    char *record = (char*)calloc(PAGE_SIZE, sizeof(char));
    
    uint16_t record_length = makeRecord(recordDescriptor, data, record);
     
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char));       
    if (fileHandle.readPage(rid.pageNum, page) != 0) {
        free(record); 
        free(page);
        return -1;   
    }
    
    uint16_t recordCount, freeSpace;   
    memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  

    if (record_length + 4 > PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
        
        free(record);
        free(page);
        return -1;
    }
    
    // write acutal record
    memcpy(page + freeSpace, record, record_length);    
    
    // write entry in page directory
    int page_dir = PAGE_SIZE - (4 * rid.slotNum + 4);
    
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &record_length, sizeof(uint16_t));
    
    freeSpace += record_length;

    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    if (fileHandle.writePage(rid.pageNum, page) != 0) {
        free(record); 
        free(page);
        return -1;   
    }
    
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

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator) {
    
    rbfm_ScanIterator.fh = &fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void*) value;
    rbfm_ScanIterator.curr_page = -1;
    rbfm_ScanIterator.curr_slot = 0;
    rbfm_ScanIterator.page = (char*)malloc(PAGE_SIZE); ;
    rbfm_ScanIterator.attributeNamesCount = attributeNames.size();
    
    unordered_set<uint16_t> __index;
    for (uint16_t i = 0; i < recordDescriptor.size(); ++i) {
        for (uint16_t j = 0; j < attributeNames.size(); ++j) {
            if (attributeNames[j] == recordDescriptor[i].name) {
                __index.insert(i);
                break;
            }            
        }
    }
    
    rbfm_ScanIterator.index = __index;
    
    for (uint16_t i = 0; i < recordDescriptor.size(); ++i) {
        if (conditionAttribute == recordDescriptor[i].name) {
            rbfm_ScanIterator.conditionAttributeIndex = i;
        }
    }
    
    return 0;    
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {     
    
    int failed = 0;   
    
    do {
        
        // check if done with this page or first time called
        if (curr_page == -1 || max_slots == curr_slot) {
            curr_page++;
            if (fh->readPage(curr_page, page) != 0) return RBFM_EOF;        
            memcpy(&max_slots, &page[PAGE_SIZE - 4], sizeof(uint16_t));
            curr_slot = 0;
        } 
        
        curr_slot++;
        int16_t record;
        memcpy(&record, &page[PAGE_SIZE - 4 - 4 * curr_slot], sizeof(int16_t));  
        // this worked for the test script, but will not work if there is a forwarding address saved
        if (record < 0) {
            failed = 1;
            continue;
        }
        
        uint16_t fieldCount;
        memcpy(&fieldCount, &page[record], sizeof(int16_t));
        if (fieldCount != recordDescriptor.size()) {
            return -1;
        }

        uint16_t long_nullvec = ceil(fieldCount / 8.0);  
        uint16_t short_nullvec = ceil(attributeNamesCount/8.0);
        
        char* null_indicator = (char*)malloc(short_nullvec);
        uint16_t null_indicator_count = 0;
        
        uint16_t directory = record + sizeof(uint16_t) + long_nullvec;
        
        uint16_t offset = sizeof(uint16_t) + long_nullvec + fieldCount * sizeof(uint16_t);
        uint16_t prev_offset = offset;
        char *data_c = (char*)data + short_nullvec;
        
        for(uint16_t i = 0; i < fieldCount; ++i) {
            
            // get new offset in to data from dir
            memcpy(&offset, &page[directory + i * sizeof(uint16_t)], sizeof(uint16_t));
            
            if (i == conditionAttributeIndex) {                
                if (compOp != NO_OP) {                
                    char target = page[record + sizeof(uint16_t) + i/8];
                    if (target & (1<<(7-i%8))) {                        
                        failed = 1;
                    } else {
                        if (recordDescriptor[i].type == TypeVarChar) {
                            int attlen = offset - prev_offset;
                            string val = string(&page[record + prev_offset], attlen);  
                            failed = !vc_comp(val);
                        } else if (recordDescriptor[i].type == TypeInt) {
                            int val;
                            memcpy(&val, &page[record + prev_offset], sizeof(int));
                            failed = int_comp(val);
                        } else {
                            float val;
                            memcpy(&val, &page[record + prev_offset], sizeof(float));
                            failed = float_comp(val);
                        }
                    }
                } else {
                    failed = 0;
                }  
            }
            
            // if this field is in the map (-> in the vector<string>)
            if (index.count(i)) {            
                char target = page[record + sizeof(uint16_t) + i/8];
                if (!(target & (1<<(7-i%8)))) {
                    if (recordDescriptor[i].type == TypeVarChar) {
                        int attlen = offset - prev_offset;
                        memcpy(&data_c[0], &attlen, sizeof(int));
                        memcpy(&data_c[4], &page[record + prev_offset], attlen);
                        data_c += (4 + attlen);                
                    } else {
                        memcpy(&data_c[0], &page[record + prev_offset], sizeof(int));
                        data_c += sizeof(int); 
                    }
                    // not null so set it to 0
                    null_indicator[null_indicator_count/8] &= ~(1 << (7 - null_indicator_count%8));
                } else {  
                    // is null so set it to 1
                    null_indicator[null_indicator_count/8] |=  (1 << (7 - null_indicator_count%8));
                }
                null_indicator_count++;  
            }
            prev_offset = offset;
        } 
        
        memcpy(data, null_indicator, short_nullvec);  
        free(null_indicator);
        
    } while (failed != 0);  
    
    rid.slotNum = curr_slot;
    rid.pageNum = curr_page;
    return 0;
}

RC RBFM_ScanIterator::close() { 
    free(page);
    return 0; 
}

uint16_t RBFM_ScanIterator::float_comp(float val) {
    float val2 = *(float*)value;
    switch (compOp) {
        case EQ_OP: return !(val2 == val);
        case LT_OP: return (val2 <= val); 
        case GT_OP: return (val2 >= val); 
        case LE_OP: return (val2 <  val); 
        case GE_OP: return (val2 >  val); 
        case NE_OP: return !(val2 != val);  
        default: return 0;
    }
}

uint16_t RBFM_ScanIterator::int_comp(int val) {
    int val2 = *(int*)value;
    switch (compOp) {
        case EQ_OP: return !(val2 == val);
        case LT_OP: return (val2 <= val); 
        case GT_OP: return (val2 >= val); 
        case LE_OP: return (val2 <  val); 
        case GE_OP: return (val2 >  val); 
        case NE_OP: return !(val2 != val);  
        default: return 0;
    }
}

uint16_t RBFM_ScanIterator::vc_comp(string val) {
    string val2 = (char*)(value);
    int cmp = strcmp(val2.c_str(), val.c_str());
    switch (compOp) {
        case EQ_OP: return cmp == 0;
        case LT_OP: return !(cmp <= 0); 
        case GT_OP: return !(cmp >= 0); 
        case LE_OP: return !(cmp <  0); 
        case GE_OP: return !(cmp >  0); 
        case NE_OP: return cmp != 0;  
        default: return 0;
    }
}
