
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
    void* newData = malloc(PAGE_SIZE);
    void* value = malloc(PAGE_SIZE);
    
    if(iter->getNextTuple(newData) == QE_EOF) {
        free(newData); free(value);
        return QE_EOF;
    }
    
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
    outer = leftIn;
    inner = rightIn;
    this->condition = condition;
    outerData = malloc(PAGE_SIZE);
    innerData = malloc(PAGE_SIZE);
    outerValue = malloc(PAGE_SIZE); 
    outer->getAttributes(outerAttrs);
    inner->getAttributes(innerAttrs);
    needNewOuterValue = true;  
    inFirstNEscan = false;           
}

RC INLJoin::getNextTuple(void *data){    
    while (true) {
        if (needNewOuterValue) {
            if (outer->getNextTuple(outerData) == QE_EOF)
                return QE_EOF;
            else 
                if (getValue(condition.lhsAttr, outerAttrs, outerData, outerValue) == IS_NULL and condition.op != NO_OP) 
                    continue;
                else {
                    initNextInnerIterator();
                    needNewOuterValue = false;
                }
        }              
        if (inner->getNextTuple(innerData) == QE_EOF) {
            if (condition.op == NE_OP and inFirstNEscan) {
                inFirstNEscan = false;
                inner->setIterator(outerValue, NULL, false, true); 
            } else {
                needNewOuterValue = true;
            }
            continue;
        } 
        break;
    }    
    // found a match
    concatData(outerAttrs, outerData, innerAttrs, innerData, data);
    return SUCCESS;
}

void INLJoin::initNextInnerIterator(void) {    
    switch (condition.op) {
        case EQ_OP: inner->setIterator(outerValue, outerValue, true,  true ); break;
        case LT_OP: inner->setIterator(NULL,       outerValue, true,  false); break; 
        case GT_OP: inner->setIterator(outerValue, NULL,       false, true ); break;
        case LE_OP: inner->setIterator(NULL      , outerValue, true,  true ); break;
        case GE_OP: inner->setIterator(outerValue, NULL,       true,  true ); break; 
        case NE_OP: inner->setIterator(NULL,       outerValue, true,  false); inFirstNEscan = true; break; 
        case NO_OP: inner->setIterator(NULL,       NULL,       true,  true ); break; 
        default: assert(false && "not a valid compop in comparision");
    }  
}

 void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();            
    for (auto &attr : outerAttrs)
        attrs.push_back(attr);
    for (auto &attr : innerAttrs)
        attrs.push_back(attr);
}
