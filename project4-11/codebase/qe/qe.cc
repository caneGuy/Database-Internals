
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

// ... the rest of your implementations go here
