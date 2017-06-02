
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    this->iterator = input;
    assert(condition.bRhsIsAttr == false && "didn't think this makes any sense");
    this->condition = condition;
    this->attrs.clear();
    input->getAttributes(this->attrs);
    this->type = condition.rhsValue.type;
    // for (auto &attr : this.attrs)
        // if (condition.lhsAttr == attr.name) {
            // this.type = attr.type;
            // return;
        // }
    // assert(false && "could not find attribute for comparison");
}

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