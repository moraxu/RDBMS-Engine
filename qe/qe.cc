#include "qe.h"
#include <iostream>
#include <typeinfo>
#include <cmath>
#include <algorithm>
#include <vector>
#include <ctime>
#include <climits>

using namespace std;

Filter::Filter(Iterator *input, const Condition &condition) {
	it = input;
	con = condition;
	input->getAttributes(attrs);
}

/*
 * Input: char *data(null indicator + actual data)
 * */
bool Filter::filterMatch(char *data){
	unsigned nullLen = (unsigned)(ceil(attrs.size()/8.0));
	char *actualData = data+nullLen;
	for(unsigned i = 0;i < attrs.size();i++){
		const char* byteInNullInfoField = data + i/8;
		bool nullField = *byteInNullInfoField & (1 << 7-i%8);
		if(attrs[i].name == con.lhsAttr) {
			if(!nullField) {
				if(attrs[i].type == TypeInt) {
					int eval = *(int *)actualData;
					return performOp(eval, *(int *)con.rhsValue.data);
				}
				else if(attrs[i].type == TypeReal) {
					float eval = *(float *)actualData;
					return performOp(eval, *(float *)con.rhsValue.data);
				}
				else {
					unsigned conLen = *(unsigned *)(con.rhsValue.data);
					unsigned tupleStrLen = *(unsigned *)actualData;
					string eval = string(actualData+sizeof(unsigned),tupleStrLen);
					return performOp(eval, string((char *)con.rhsValue.data+sizeof(unsigned),conLen));
				}
			}
			else {
                return false;
			}
		}
		else if(!nullField) {
            if(attrs[i].type == TypeInt) {
                actualData += sizeof(int);
            }
            else if(attrs[i].type == TypeReal) {
                actualData += sizeof(float);
            }
            else{
                unsigned strLen = *(unsigned *)actualData;
                actualData += (sizeof(unsigned)+strLen);
            }
		}
	}
	// If the filtered field can't be found,return false.
	return false;
}

template <typename T>
bool Filter::performOp(const T &x,const T &y){
	switch(con.op) {
		case EQ_OP: return x == y;
		case LT_OP: return x < y;
		case LE_OP: return x <= y;
		case GT_OP: return x > y;
		case GE_OP: return x >= y;
		case NE_OP: return x != y;
		case NO_OP: return false;
		default: return false;
	}
}

/*
 * The intermediate tuples,as the result of last query tree node,are got by calling the node's getNextTuple().
 * Then it serves as the input of current query tree node.
 * */
RC Filter::getNextTuple(void *data){
	//Filter this tuple got from other operators or tableScan/IndexScan
	int rc = it->getNextTuple(data);
	if(rc != 0) {
        return QE_EOF;
	}
	while(!filterMatch((char *)data)) {
		rc = it->getNextTuple(data);
		if(rc != 0) {
            return QE_EOF;
		}
	}
	return 0;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const {
	it->getAttributes(attrs);
}

//attrNames contains its table name!
//Assume attrNames is a subset of table attribute names.
Project::Project(Iterator *input,const std::vector<std::string> &attrNames){
	this->it = input;
	this->projectedAttrName = attrNames;

	// Initialize projected attribute vector
	projectedAttr.clear();
	vector<Attribute> allAttrs;
	it->getAttributes(allAttrs);
	for(int i = 0;i < projectedAttrName.size();i++){
		for(int j = 0;j < allAttrs.size();j++){
			if(allAttrs[j].name == projectedAttrName[i]){
				projectedAttr.push_back(allAttrs[j]);
			}
		}
	}
	//getAttributes(this->projectedAttr);
	//cout<<"projectedAttr size:"<<projectedAttr.size()<<endl;
}

void Project::preprocess(char *pre,char *post){
	vector<Attribute> allAttrs;
	it->getAttributes(allAttrs);

	std::vector<unsigned> fieldOffsets(allAttrs.size()+1,0); // Record field offset in pre
    const unsigned nullFieldLen = (unsigned)(ceil(allAttrs.size()/8.0));
	const char* actualData = pre + nullFieldLen;
	unsigned actualDataLen = 0;
	for(unsigned i = 0 ; i < allAttrs.size() ; ++i) {
		const char* byteInNullInfoField = pre + i/8;

		bool nullField = *byteInNullInfoField & (1 << 7-i%8);
		if(nullField) {
			fieldOffsets[i+1] = fieldOffsets[i];
		}
		else {
			if(allAttrs[i].type == AttrType::TypeInt || allAttrs[i].type == AttrType::TypeReal) {
				fieldOffsets[i+1] = fieldOffsets[i] + allAttrs[i].length;
				actualDataLen += allAttrs[i].length;
			}
			else { //recordDescriptor[i].type == AttrType::TypeVarChar
				unsigned varCharLength = *reinterpret_cast<const unsigned*>(actualData + actualDataLen);
				fieldOffsets[i+1] = fieldOffsets[i] + (4 + varCharLength);
				actualDataLen += (4 + varCharLength);
			}
		}
	}

	unsigned projectedNullFieldLen = (unsigned)(ceil(projectedAttr.size()/8.0));
	memset(post, 0, projectedNullFieldLen); // By default no field is NULL
	char *cur = post+projectedNullFieldLen;
	for(int i = 0;i < projectedAttrName.size();i++){
		for(int j = 0;j < allAttrs.size();j++){
			if(allAttrs[j].name == projectedAttrName[i]){
				unsigned currByte = i/8;
				if(fieldOffsets[j+1] == fieldOffsets[j])
					*(post+currByte) |= (1 << 7-i%8);
				else{
					memcpy(cur, pre+nullFieldLen+fieldOffsets[j], fieldOffsets[j+1]-fieldOffsets[j]);
					cur += fieldOffsets[j+1]-fieldOffsets[j];
				}
			}
		}
	}
}

RC Project::getNextTuple(void *data){
	char pre[PAGE_SIZE];
	int rc = it->getNextTuple(pre);
	if(rc != 0)
		return rc;
	preprocess(pre, (char *)data);
	return 0;
}

void Project::getAttributes(std::vector<Attribute> &attrs) const {
	attrs = projectedAttr;
}

/*
 * This implementation doesn't involve in-memory hash table.
 * */
BNLJoin::BNLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition,
            const unsigned numPages){
	left = leftIn;
	right = rightIn;
	con = condition;
	left->getAttributes(leftAttrs);
	right->getAttributes(rightAttrs);
	getAttributes(attrs);
	bufferPage = numPages;
	leftTable = (char *)malloc(bufferPage*PAGE_SIZE);
	rightTable = (char *)malloc(PAGE_SIZE);
	currLOffset = 0;
	currROffset = 0;
	//rightRid = {0,0};
	loadLeftTable();
	loadRightPage();
	endOfFile = false;
}

BNLJoin::~BNLJoin(){
	delete leftTable;
	delete rightTable;
}

/*
 * NOTE: Left "table" is not necessarily a table,it can be a intermediate query result
 * Data format: null field+actual data
 */
RC BNLJoin::loadLeftTable(){
	unsigned currLen = 0;
	unsigned nullLen = ceil(leftAttrs.size()/8.0);
	while(currLen < bufferPage*PAGE_SIZE){
		int rc = left->getNextTuple(leftTable+currLen);
		if(rc != 0){
			leftOffset = currLen;
			return rc;
		}
		char *actualData = leftTable+currLen+nullLen;
		for(unsigned i = 0;i < leftAttrs.size();i++){
			const char* byteInNullInfoField = leftTable+currLen + i/8;

			bool nullField = *byteInNullInfoField & (1 << 7-i%8);
			if(!nullField)
				if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal)
					actualData += sizeof(int);
				else{
					unsigned strLen = *(unsigned *)actualData;
					actualData += (sizeof(unsigned)+strLen);
				}
		}
		currLen += (actualData-leftTable-currLen);
	}
	leftOffset = currLen;

	return 0;
}

RC BNLJoin::loadRightPage(){
	unsigned currLen = 0;
	unsigned nullLen = ceil(rightAttrs.size()/8.0);
	while(currLen < PAGE_SIZE){
		int rc = right->getNextTuple(rightTable+currLen);
		if(rc != 0){
			rightOffset = currLen;
			return rc;
		}
		char *actualData = rightTable+currLen+nullLen;
		for(unsigned i = 0;i < rightAttrs.size();i++){
			const char* byteInNullInfoField = rightTable+currLen + i/8;

			bool nullField = *byteInNullInfoField & (1 << 7-i%8);
			if(!nullField)
				if(rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal)
					actualData += sizeof(int);
				else{
					unsigned strLen = *(unsigned *)actualData;
					actualData += (sizeof(unsigned)+strLen);
				}
		}
		currLen += (actualData-rightTable-currLen);
	}
	rightOffset = currLen;

	return 0;
}

/*RC BNLJoin::loadRightPage(){
	FileHandle fileHandle;
	int rc = right->rm.openFile(right->tableName,fileHandle);
	if(rc != 0)
		return rc;

	unsigned noPages = fileHandle.getNumberOfPages();
	//All pages of right table have been read.
	if(rightRid.pageNum == noPages){
		right->rm.closeFile(fileHandle);
		return -2;
	}
	rc = fileHandle.readPage(rightRid.pageNum, rightTable);
	if(rc != 0){
		right->rm.closeFile(fileHandle);
		return rc;
	}
	right->rm.closeFile(fileHandle);
	return 0;
}*/

/*
 * This function moves current pointers to the next pair of tuples to be compared.
 * It returns QE_EOF when every tuple of left 'table' has been read.
 * */
RC BNLJoin::moveToNextMatchingPairs(const unsigned preLeftTupLen,const unsigned preRightTupLen){
	int rc = 0;
	// Maintain leftRid and rightRid to match next tuple
	//unsigned rightSlotSize = *(unsigned *)(rightTable+PAGE_SIZE-2*sizeof(unsigned));
	currROffset += preRightTupLen;
	if(currROffset >= rightOffset){
		currROffset = 0;
		currLOffset += preLeftTupLen;
		if(currLOffset >= leftOffset){
			currLOffset = 0;
			rc = loadRightPage();
			// If end of right table is reached,the next block of left table should be loaded.
			if(rc != 0){
				rc = loadLeftTable();
				right->setIterator();
				loadRightPage();
			}
		}
	}
	if(rc != 0) return QE_EOF;
	else return 0;
}

template <typename T>
bool BNLJoin::performOp(const T &x,const T &y){
	bool res;
	switch(con.op){
		case EQ_OP: res = x == y;break;
		case LT_OP: res = x < y;break;
		case LE_OP: res = x <= y;break;
		case GT_OP: res = x > y;break;
		case GE_OP: res = x >= y;break;
		case NE_OP: res = x != y;break;
		case NO_OP: res = false;break;
		default: res = false;break;
	}
	return res;
}


/*
 * Two tuples don't match each other when:
 * 1) Condition is not set to match two tuples;
 * 2) No attribute with the same name as in Condition variable can be found;
 * 3) The corresponding fields are found but they don't match in value.
 * This function also calculates the length of two tuples,reserved as its parameteres.
 * */
bool BNLJoin::BNLJoinMatch(unsigned &leftTupleLen,unsigned &rightTupleLen){
	if(!con.bRhsIsAttr)
		return false; // Condition variable is not set to compared two fields.
	int ivalL,ivalR;
	float fvalL,fvalR;
	string strL,strR;
	bool res = false;
	unsigned nullLen = ceil(leftAttrs.size()/8.0);
	char *actualData = leftTable+currLOffset+nullLen;
	for(int i = 0;i < leftAttrs.size();i++){
		const char* byteInNullInfoField = leftTable+currLOffset + i/8;

		bool nullField = *byteInNullInfoField & (1 << 7-i%8);
		if(!nullField){
			if(leftAttrs[i].name == con.lhsAttr){
				if(leftAttrs[i].type == TypeInt){
					ivalL = *(int *)actualData;
				}else if(leftAttrs[i].type == TypeReal){
					fvalL = *(float *)actualData;
				}else{
					unsigned tupleStrLen = *(unsigned *)actualData;
					strL = string(actualData+sizeof(unsigned),tupleStrLen);
				}
			}
			if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal)
				actualData += sizeof(int);
			else{
				unsigned strLen = *(unsigned *)actualData;
				actualData += (sizeof(unsigned)+strLen);
			}
		}
	}
	leftTupleLen = actualData-leftTable-currLOffset;

	nullLen = ceil(rightAttrs.size()/8.0);
	actualData = rightTable+currROffset+nullLen;
	for(int i = 0;i < rightAttrs.size();i++){
		const char* byteInNullInfoField = rightTable+currROffset + i/8;

		bool nullField = *byteInNullInfoField & (1 << 7-i%8);
		if(!nullField){
			if(rightAttrs[i].name == con.rhsAttr){
				if(rightAttrs[i].type == TypeInt){
					ivalR = *(int *)actualData;
					res = performOp(ivalL, ivalR);
				}else if(rightAttrs[i].type == TypeReal){
					fvalR = *(float *)actualData;
					res = performOp(fvalL, fvalR);
				}else{
					unsigned tupleStrLen = *(unsigned *)actualData;
					strR = string(actualData+sizeof(unsigned),tupleStrLen);
					res = performOp(strL, strR);
				}
			}
			if(rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal)
				actualData += sizeof(int);
			else{
				unsigned strLen = *(unsigned *)actualData;
				actualData += (sizeof(unsigned)+strLen);
			}
		}
	}
	rightTupleLen = actualData-rightTable-currROffset;

	return res;

	/*unsigned rightTupleOffset = *(unsigned *)(rightTable+PAGE_SIZE-2*(rightRid.slotNum+2)*sizeof(unsigned));
	char *rightTuple = rightTable+rightTupleOffset;
	for(int i = 0;i < rightAttrs.size();i++){
		if(rightAttrs[i].name == con.rhsAttr){
			unsigned offset = *(unsigned *)(rightTuple+i*sizeof(unsigned));
			if(rightAttrs[i].type == TypeInt){
				ivalR = *(int *)(rightTuple+(rightAttrs.size()+1)*sizeof(unsigned)+offset);
				return performOp(ivalL, ivalR);
			}else if(rightAttrs[i].type == TypeReal){
				fvalR = *(float *)(rightTuple+(rightAttrs.size()+1)*sizeof(unsigned)+offset);
				return performOp(fvalL, fvalR);
			}else{
				unsigned tupleStrLen = *(unsigned *)(rightTuple+(rightAttrs.size()+2)*sizeof(unsigned)+offset);
				strR = string(rightTuple+(rightAttrs.size()+2)*sizeof(unsigned)+offset,tupleStrLen);
				return performOp(strL, strR);
			}
		}
	}*/
}

void BNLJoin::BNLJoinTuple(void *data,const unsigned leftTupleLen,const unsigned rightTupleLen){
	unsigned leftNullField = ceil(leftAttrs.size()/8.0);
	unsigned rightNullField = ceil(rightAttrs.size()/8.0);
	unsigned nullField = ceil((leftAttrs.size()+rightAttrs.size())/8.0); // Null field length of joined tuple
	char *cur = (char *)data;
	unsigned shiftBits = leftNullField*8-leftAttrs.size();
	memcpy(cur, leftTable+currLOffset, leftNullField);
	cur += leftNullField;
	memcpy(cur, rightTable+currROffset, rightNullField);
	/*
	 * Now our task is to merge these two null fields in the scale of bits.
	 * shiftBits gives the number of bits that right null field should (left)shift with.
	 * But I'm not sure how to implement it.
	 * */
	char firstByte = *(rightTable+currROffset);
	char mask = 0;
	for(int i = 8-shiftBits;i < 8;i++)
		mask += pow(2,i);
	firstByte &= mask;
	firstByte >>= shiftBits;
	*(cur-1) |= firstByte;
	for(int i = shiftBits;i < rightAttrs.size();i++){
		const char* rightNullPointer = rightTable+currROffset + i/8;
		bool nullField = *rightNullPointer & (1 << 7-i%8);
		if(nullField)
			*(cur+(i-shiftBits)/8) |= (1 << 7-(i+shiftBits)%8);
	}

	cur = (char *)data+nullField;
	memcpy(cur, leftTable+currLOffset+leftNullField, leftTupleLen-leftNullField);
	cur += leftTupleLen-leftNullField;
	memcpy(cur, rightTable+currROffset+rightNullField, rightTupleLen-rightNullField);
}

RC BNLJoin::getNextTuple(void *data){
	// Match and join
	unsigned leftTupleLen,rightTupleLen;
	if(endOfFile) return QE_EOF;
	while(!BNLJoinMatch(leftTupleLen,rightTupleLen)){
		int rc = moveToNextMatchingPairs(leftTupleLen,rightTupleLen);
		if(rc != 0)
			return QE_EOF;
	}

	BNLJoinTuple(data,leftTupleLen,rightTupleLen);
	int rc = moveToNextMatchingPairs(leftTupleLen, rightTupleLen);
	if(rc != 0)
		endOfFile = true;
	return 0;
}

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = leftAttrs;

	for(auto attribute : rightAttrs)
		attrs.push_back(attribute);
}

Aggregate::Aggregate(Iterator *input,const Attribute &aggAttr,AggregateOp op){
	it = input;
	this->op = op;
	it->getAttributes(attributes);
	aggrAttr = aggAttr;
	scanned = false;
}

RC Aggregate::getNextTuple(void *data){
	if(!scanned){
		float res = 0;
		int cnt = 0;
		if(op == MIN) res = INT_MAX;
		if(op == MAX) res = INT_MIN;
		while(it->getNextTuple(data) != QE_EOF){
			unsigned nullFieldLen = (unsigned)ceil(attributes.size()/8.0);
			char *cur = (char *)data+nullFieldLen;
			for(unsigned i = 0;i < attributes.size();i++){
				const char* nullByte = (char *)data + i/8;

				bool nullField = *nullByte & (1 << 7-i%8);
				if(!nullField && attributes[i].name == aggrAttr.name){
					if(attributes[i].type == TypeInt){
						switch(op){
							case MIN: if(*(int *)cur < res) res = *(int *)cur;break;
							case MAX: if(*(int *)cur > res) res = *(int *)cur;break;
							case COUNT: res++;break;
							case SUM: res += *(int *)cur;break;
							case AVG: res += *(int *)cur;cnt++;break;
						}
					}else if(attributes[i].type == TypeReal){
						switch(op){
							case MIN: if(*(float *)cur < res) res = *(int *)cur;break;
							case MAX: if(*(float *)cur > res) res = *(int *)cur;break;
							case COUNT: res++;break;
							case SUM: res += *(float *)cur;break;
							case AVG: res += *(float *)cur;cnt++;break;
						}
					}
					break;
				}else if(!nullField){
					if(attributes[i].type == TypeInt || attributes[i].type == TypeReal)
						cur += sizeof(int);
					else{
						unsigned strLen = *(unsigned *)cur;
						cur += strLen+sizeof(unsigned);
					}
				}
			}
		}

		if(op == AVG)
			res /= (float)cnt;
		unsigned aggregateNullLen = 1;
		memset((char *)data, 0, aggregateNullLen);
		*(float *)((char *)data+1) = res;
		scanned = true;
		return 0;
	}else{
		return QE_EOF;
	}
}

void Aggregate::setAttribute(Attribute &attrs) const{
	switch(op){
		case MIN: attrs.name = "MIN("+attrs.name;break;
		case MAX: attrs.name = "MAX("+attrs.name;break;
		case COUNT: attrs.name = "COUNT("+attrs.name;break;
		case SUM: attrs.name = "SUM("+attrs.name;break;
		case AVG: attrs.name = "AVG("+attrs.name;break;
	}
	attrs.name += ")";
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const{
	attrs = attributes;
	for(int i = 0;i < attrs.size();i++)
		if(attrs[i].name == aggrAttr.name)
			setAttribute(attrs[i]);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) : rm(RelationManager::instance()) {
    left = leftIn;
    right = rightIn;

    tempTableName = "temp_"+to_string(time(NULL));
    left->getAttributes(attrs);
    vector<Attribute> rightAttrs;
    right->getAttributes(rightAttrs);
    for(auto& attribute : rightAttrs)
        attrs.push_back(attribute);

    RC rc = rm.createTable(tempTableName, attrs);
    if(rc != 0) {
        return;
    }


}

INLJoin::~INLJoin() {
    rm.deleteTable(tempTableName);
}

RC INLJoin::getNextTuple(void *data) {

}

void INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();
    left->getAttributes(attrs);
    vector<Attribute> rightAttrs;
    right->getAttributes(rightAttrs);

    for(auto& attribute : rightAttrs)
        attrs.push_back(attribute);
}