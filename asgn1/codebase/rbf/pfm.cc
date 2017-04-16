#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    struct stat fileInfo;
    if(stat(fileName.c_str(), &fileInfo) == 0) return -1;
    
    FILE* file;
    file = fopen(fileName.c_str(), "w");
    if(file == NULL) return -1;
    
    fclose(file);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    struct stat fileInfo;
    if(stat(fileName.c_str(), &fileInfo) != 0) return -1;

    if(remove(fileName.c_str()) != 0) return -1;

    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(fileHandle.openedFile != NULL) return -1;
    
    struct stat fileInfo;
    if(stat(fileName.c_str(), &fileInfo) != 0) return -1;
    
    fileHandle.openedFile = fopen(fileName.c_str(), "a+");
    if(fileHandle.openedFile == NULL) return -1;
    
    fileHandle.fileSize = fileInfo.st_size;
    
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(fileHandle.openedFile == NULL) return -1;
    
    fclose(fileHandle.openedFile);
    
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fileSize = 0;
    openedFile = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    unsigned long totalPages = fileSize/PAGE_SIZE;
    if(pageNum >= totalPages) return 2;

    if(fseek(openedFile, pageNum * PAGE_SIZE, SEEK_SET) != 0) return 3;
    char buffer[PAGE_SIZE];

    if(fgets(buffer, PAGE_SIZE, openedFile) == NULL) return 4;
    if(memcpy((char*) data, buffer, PAGE_SIZE) != data) return 6;

    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    if(fseek(openedFile, 0, SEEK_END) != 0)  return -1;
    if(fwrite(data, sizeof(char), PAGE_SIZE/sizeof(char), openedFile) != PAGE_SIZE/sizeof(char)) return -1;
    
    fileSize += PAGE_SIZE;

    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    return fileSize/PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
