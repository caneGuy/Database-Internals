#ifndef _qe_h_
#define _qe_h_

// #include <string>
// #include <vector>
// #include <stdio.h>

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <assert.h> 
#include <math.h> 

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan
#define IS_NULL (-1)  // column entry is null

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
        
    protected:                
        int getValue(const string name, const vector<Attribute> &attrs, const void* data, void* value) {
            int offset = ceil(attrs.size() / 8.0);
            for (size_t i = 0; i < attrs.size(); ++i) {
                char target = *((char*)data + i/8);
                if (target & (1<<(7-i%8))) {
                    if (name == attrs[i].name)
                        return IS_NULL;
                    else 
                        continue;
                }
                int size = sizeof(int);
                if (attrs[i].type == TypeVarChar) {
                    memcpy(&size, (char*)data + offset, sizeof(int));
                    memcpy((char*)value, &size, sizeof(int));
                    memcpy((char*)value + sizeof(int), (char*)data + offset + sizeof(int), size);
                    size += sizeof(int);
                } else 
                    memcpy(value, (char*)data + offset, sizeof(int));                 
                if (name == attrs[i].name)
                    return size;     
                offset += size;   
            }
            assert(false && "getValue called wiht name that is not in list");
            return 0;
        };    
        
        int getSize(const vector<Attribute> &attrs, const void* data) {
            int offset = ceil(attrs.size() / 8.0);
            for (size_t i = 0; i < attrs.size(); ++i) {
                char target = *((char*)data + i/8);
                if (target & (1<<(7-i%8))) 
                    continue;
                if (attrs[i].type == TypeVarChar) {
                    int size;
                    memcpy(&size, (char*)data + offset, sizeof(int));
                    offset += size;
                }      
                offset += sizeof(int);
            }
            return offset;
        };
        
        RC compare(CompOp op, AttrType type, void* left, void* right) {            
            switch (type) {
                case TypeVarChar: {
                    int size;
                    memcpy(&size, left, sizeof(int));
                    char c_left[size + 1];
                    memcpy(c_left, (char*)left + 4, size);
                    c_left[size] = 0;
                    memcpy(&size, right, sizeof(int));
                    char c_right[size + 1];
                    memcpy(c_right, (char*)right + 4, size);
                    c_right[size] = 0; 
                    return comp<string>(op, c_left, c_right);
                }
                case TypeInt: return comp<int>(op, *(int*)left, *(int*)right);
                case TypeReal: return comp<float>(op, *(float*)left, *(float*)right);
                default:
                    assert(false && "not a valid type in comparision");
            }            
        };
                
        template <class T>
        bool comp (CompOp op, T left, T right) {
            switch (op) {
                case EQ_OP: return (left == right);
                case LT_OP: return (left <  right); 
                case GT_OP: return (left >  right); 
                case LE_OP: return (left <= right); 
                case GE_OP: return (left >= right); 
                case NE_OP: return (left != right);  
                case NO_OP: return true;
                default: assert(false && "not a valid compop in comparision");
            }
        };
        
        RC concatData(vector<Attribute> outerAttrs, const void* outerData, vector<Attribute> innerAttrs, const void* innerData, void* data) {
            int outerNullSize = ceil(outerAttrs.size() / 8.0);
            int innerNullSize = ceil(innerAttrs.size() / 8.0);
            int totalNullSize = ceil((outerAttrs.size() + innerAttrs.size()) / 8.0);
            memcpy(data, outerData, outerNullSize);
            // do some stuff            
            for (size_t i = 0; i < innerAttrs.size(); ++i) {
                size_t k = i + outerAttrs.size();
                char* target = (char*)data + (k)/8;
                char origin = *((char*)innerData + i/8);
                if (origin & (1<<(7-i%8))) 
                    *target |= (1<<(7-k%8));
                else 
                    *target &= ~(1<<(7-k%8));
            }
            int outerSize = getSize(outerAttrs, outerData) - outerNullSize;
            int innerSize = getSize(innerAttrs, innerData) - innerNullSize;
            memcpy((char*)data + totalNullSize, (char*)outerData + outerNullSize, outerSize);
            memcpy((char*)data + totalNullSize + outerSize, (char*)innerData + innerNullSize, innerSize);
            return 0;
        };  
        
    public:
        void printData(AttrType type, void* data) {
            switch (type) {
                case TypeVarChar: {
                    int size;
                    memcpy(&size, data, sizeof(int));
                    char c_left[size + 1];
                    memcpy(c_left, (char*)data + 4, size);
                    cout << c_left << endl;
                }
                case TypeInt: cout << *(int*)data << endl;
                case TypeReal: cout << *(float*)data << endl;
            }
        };
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
            delete iter;
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
            delete iter;
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Iterator *iterator;
        Condition condition;
        vector<Attribute> attrs;
        void* buffer;
        
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        ){
            this->iterator = input;
            assert(condition.bRhsIsAttr == false && "don't think this makes any sense");
            this->condition = condition;
            this->attrs.clear();
            input->getAttributes(this->attrs);
            this->buffer = malloc(PAGE_SIZE);
        };
        ~Filter(){
            free(this->buffer);
        };

        RC getNextTuple(void *data) { 
            while (true) {
                if (iterator->getNextTuple(data) == QE_EOF)
                    return QE_EOF;
                if (condition.op == NO_OP)
                    break;
                if (getValue(condition.lhsAttr, attrs, data, this->buffer) == IS_NULL)
                    continue;
                if (compare(condition.op, condition.rhsValue.type, this->buffer, condition.rhsValue.data))
                    break;
            }
            return SUCCESS;
        };
        
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
            attrs.clear();
            attrs = this->attrs;
        };   

};


class Project : public Iterator {
    // Projection operator
    public:
        vector<string> attrNames;
        Iterator* iter;
        vector<Attribute> attrs;
        Project(Iterator *input,                  // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

// Optional for everyone. 10 extra-credit points
class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){};
        ~BNLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:        
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin(){
            free(outerData);
            free(innerData);
            free(outerValue);
        };

        RC getNextTuple(void *data);
        
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        
    private:
        Iterator *outer;
        IndexScan *inner;
        Condition condition;
        vector<Attribute> outerAttrs;
        vector<Attribute> innerAttrs;
        void* outerData;
        void* outerValue;
        void* innerData;
        bool inFirstNEscan;     
        bool needNewOuterValue;
        AttrType type;
        
        void initNextInnerIterator(void);
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
                Iterator *rightIn,               // Iterator of input S
                const Condition &condition,      // Join condition (CompOp is always EQ)
                const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        ){};
        ~GHJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){};

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op               // Aggregate operation
        ){};
        ~Aggregate(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const{};
};

#endif
