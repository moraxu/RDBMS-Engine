#ifndef _qe_h_
#define _qe_h_

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <iostream>

using namespace std;

#define QE_EOF (-1)  // end of the index scan

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

/*class iterable{
	char *cont;
	unsigned len;
	vector<unsigned> offset;
	vector<Attribute> attrs;

public:
	bool operator()(const iterable& x, const iterable& y){
		unsigned fieldLen = offset.size()-1;
		for(unsigned i = 0;i < fieldLen;i++){
			if(attrs[i].type == TypeInt){
				int xCont = *(int *)(x.cont+x.offset[i]);
				int yCont = *(int *)(y.cont+y.offset[i]);
				if(xCont < yCont) return true;
				else if(xCont > yCont) return false;
			}else if(attrs[i].type == TypeReal){
				float xCont = *(float *)(x.cont+x.offset[i]);
				float yCont = *(float *)(y.cont+y.offset[i]);
				if(xCont < yCont) return true;
				else if(xCont > yCont) return false;
			}else{
				string xCont = string(x.cont+x.offset[i],y.offset[i+1]-y.offset[i]);
				string yCont = string(y.cont+y.offset[i],y.offset[i+1]-y.offset[i]);
				if(xCont < yCont) return true;
				else if(xCont > yCont) return false;
			}
		}
		return false;
	}
}; // This struct is used to convert binary streams of records into iterables so they can be sorted
*/

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;
};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        //cerr<<"Use rm scan to init..."<<endl;
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName, rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;


        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
    };
};

class Filter : public Iterator {
    // Filter operator
	Iterator *it;
	Condition con;
	vector<Attribute> attrs;
public:
    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );

    ~Filter() override = default;

    template <typename T>
    bool performOp(const T &x,const T &y);

    bool filterMatch(char *data);

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class Project : public Iterator {
    // Projection operator
	Iterator *it;
	vector<string> projectedAttrName;
	vector<Attribute> projectedAttr;
	string tableName;
public:
    Project(Iterator *input,                    // Iterator of input R
            const std::vector<std::string> &attrNames);   // std::vector containing attribute names
    ~Project() override = default;

    void preprocess(char *pre,char *post);

    //void convertBinaryToIterable(vector<iterable> &v,char *data);

    //void covertIterableToBinary(const vector<iterable> v,char *data);

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
	Iterator *left;
	TableScan *right;
	Condition con;
	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;
	vector<Attribute> attrs;
	unsigned bufferPage; // Buffer pool size
	char *leftTable; // Memory buffer allocated for left table(size of bufferPage*PAGE_SIZE)
	char *rightTable; // Memory buffer allocated for right table(PAGE_SIZE)
	unsigned leftOffset; // Actual largest offset of loaded left table
	unsigned currLOffset; // Current offset in page leftPageNum
	unsigned rightOffset;
	unsigned currROffset;
	unsigned maxLeft;
	unsigned maxRight;
	bool endOfFile; // Used in getNextTuple() to help determine whether it should return QE_EOF
	//RID rightRid; // Current rid of right table to be checked for joining

public:
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    );

    ~BNLJoin() override;

    unsigned calcMaxLenOfTuple(vector<Attribute> attrs);

    RC loadLeftTable();

    RC loadRightPage();

    RC moveToNextMatchingPairs(const unsigned preLeftTupLen,const unsigned preRightTupLen);

    template <typename T>
    bool performOp(const T &x,const T &y);

    bool BNLJoinMatch(unsigned &leftTupleLen,unsigned &rightTupleLen);

    void BNLJoinTuple(void *data,const unsigned leftTupleLen,const unsigned rightTupleLen);

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
    RelationManager& rm;
    RM_ScanIterator iter;
    Iterator *left;
    IndexScan *right;
    string tempTableName;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<Attribute> attrs;

    RC extractField(const byte* record, const std::vector<Attribute> &attrs, const string& fieldToExtract, std::vector<byte>& extractedField);
    void concatenateRecords(const byte* firstRecord, const std::vector<Attribute> &firstRecordAttrs,
                            const byte* secondRecord, const std::vector<Attribute> &secondRecordAttrs,
                            std::vector<byte>& result);

public:
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );

    ~INLJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    ) {};

    ~GHJoin() override = default;

    RC getNextTuple(void *data) override { return QE_EOF; };

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {};
};

class Aggregate : public Iterator {
    // Aggregation operator
	Iterator *it;
	Attribute aggrAttr;
	AggregateOp op;
	vector<Attribute> attributes;
	bool scanned;

public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    );

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    );

    ~Aggregate() override = default;

    RC getNextTuple(void *data) override;

    void setAttribute(Attribute &attrs) const;

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

#endif
