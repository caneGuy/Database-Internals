
#include "qe.h"


Project::Project(Iterator *input, const vector<string> &attrNames)
{
    // Set members
    this->attrNames = attrNames;
    this->iter = input;
    input->getAttributes(this->attrs);
};


RC Project::getNextTuple(void* data) {

    void* newData = malloc(sizeof(data));
    void* value = malloc(sizeof(data));

    if(iter->getNextTuple(newData) == QE_EOF) {
        return QE_EOF;
    }
    // double check this calculation
    int offset = 1+(attrNames.size()/8);
    int rc;
    int i = 0;
    int amt = 0;

    while(i < attrNames.size()) {
        char* target = (char*)data + (char)(i/8);
        rc = getValue(attrNames[i], attrs, newData, value);
        if(rc == IS_NULL) {
            *target |= 1 << 7-(i%8);
        } else {
            *target &= ~(1 << (7-(i%8)));
            memcpy(data+offset, value, rc);
            cout << offset << endl;
            offset += rc;
        }
        i++;
    }
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    iter->getAttributes(attrs);
}
