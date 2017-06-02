
#include "qe.h"


Project::Project(Iterator *input, const vector<string> &attrNames)
{
    // Set members
    this->attrNames = attrNames;
    this->iter = input;
    input->getAttributes(this->attrs);
};


RC Project::getNextTuple(void* data) {
    // check this sizeof()!
    void* newData = malloc(sizeof(data));
    void* value = malloc(sizeof(data));
    
    if(iter->getNextTuple(newData) == QE_EOF)
        return QE_EOF;
    
    int offset = ceil(attrNames.size()/8.0);
    for (size_t i = 0; i < attrNames.size(); ++i) {
        char* target = (char*)data + (char)(i/8);
        int lenght = getValue(attrNames[i], attrs, newData, value);
        if(lenght == IS_NULL) {
            *target |= (1 << (7-i%8));
        } else {
            *target &= ~(1 << (7-i%8));
            memcpy((char*)data + offset, value, lenght);
            offset += lenght;
        }
    }  
    
    free(newData);
    free(value);
    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    for (auto &name : this->attrNames)
        for (auto &attr : this->attrs)
            if (attr.name == name)
                attrs.push_back(attr);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    this->outer = leftIn;
    this->inner = rightIn;
    this->condition = condition;
    this->outerData = malloc(PAGE_SIZE);
    this->innerData = malloc(PAGE_SIZE);
    this->outerValue = malloc(PAGE_SIZE);
    this->needNewOuterValue = true;   
    outer->getAttributes(this->outerAttrs);
    inner->getAttributes(this->innerAttrs);
    for (auto &attr : this->outerAttrs)
        if (condition.lhsAttr == attr.name) {
            this->type = attr.type;
            return;
        }
    assert(false && "could not find attribute for comparison");            
}

RC INLJoin::getNextTuple(void *data){      
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();      
    while (true) {
        if (needNewOuterValue) {
            if (outer->getNextTuple(outerData) == QE_EOF)
                return QE_EOF;
            else 
                if (getValue(condition.lhsAttr, outerAttrs, outerData, outerValue) == IS_NULL) 
                    continue;
                else
                    needNewOuterValue = false;
            rbfm->printRecord(outerAttrs, outerData);
        }
        if (inner->getNextTuple(innerData) == QE_EOF) {
            inner->setIterator();
            needNewOuterValue = true;
            continue;
        }
        rbfm->printRecord(innerAttrs, innerData);
        // assuming we don't want the row if attr is null
        if (getValue(condition.rhsAttr, innerAttrs, innerData, data) == IS_NULL)
            continue;
        if (compare(condition.op, type, outerValue, data))
            break;
    }
    concatData(outerAttrs, outerData, innerAttrs, innerData, data);
    vector<Attribute> temp;
    this->getAttributes(temp);
    rbfm->printRecord(temp, data);
    return SUCCESS;
}

 void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();            
    for (auto &attr : outerAttrs)
        attrs.push_back(attr);
    for (auto &attr : innerAttrs)
        attrs.push_back(attr);
}
