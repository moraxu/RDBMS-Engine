#include "rm.h"
#include <cmath>

using namespace std;

RelationManager *RelationManager::_relation_manager = nullptr;

//Change return type to RelationManager *
RelationManager *RelationManager::instance() {
    if (!_relation_manager)
        _relation_manager = RelationManager::new RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager(){
    rbfm = RecordBasedFileManager();
    createTableDescriptor();
    createColumnDescriptor();
    createCatalog();
    lastTableID = 0;
    numberOfColumnsTblFields = 5;
}

RelationManager::~RelationManager() {
    delete _relation_manager;
    deleteCatalog();
}

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

/******************************************************************************************************
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
******************************************************************************************************/
RC RelationManager::createTableDescriptor(){
    vector<string> fieldName = {"table-id","table-name","file-name"};
    vector<int> type = {TypeInt,TypeVarChar,TypeVarChar};
    vector<int> len = {sizeof(int),50,50};

    for(int i = 0;i < 3;i++){
        Attribute tmp;
        tmp.name = fieldName[i];
        tmp.type = type[i];
        tmp.length = len[i];
        tablesDescriptor.push_back(tmp);
    }

    return 0;
}

/******************************************************************************************************
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
******************************************************************************************************/
RC RelationManager::createColumnDescriptor(){
    vector<string> fieldName = {"table-id","column-name","column-type","column-length","column-position"};
    vector<int> type = {TypeInt,TypeVarChar,TypeInt,TypeInt,TypeInt};
    vector<int> len = {sizeof(int),50,sizeof(int),sizeof(int),sizeof(int)};

    for(int i = 0;i < 5;i++){
        Attribute tmp;
        tmp.name = fieldName[i];
        tmp.type = type[i];
        tmp.length = len[i];
        columnDescriptor.push_back(tmp);
    }

    return 0;
}

RC RelationManager::openFile(const std::string &tableName,FileHandle &fileHandle){
    string fn = getFileName(tableName);
    //No such file associated with tableName
    if(fn.empty())
        return -1;

    return rbfm.openFile(fn,fileHandle);
}

RC RelationManager::createFile(const std::string &fileName){
    return rbfm.createFile(fileName);
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
NO FILE ASSOCIATED WITH TABLENAME: -2
**************************************/
int RelationManager::getIdFromTableName(const std::string &tableName){
    FileHandle fh;
    int rc = rbfm.openFile("Tables",fh);
    if(rc != 0)
        return rc;

    byte page[PAGE_SIZE];
    RID rid;
    string res;
    RBFM_ScanIterator it = RBFM_ScanIterator();
    std::vector<string> attr = {"table-id","table-name","file-name"};
    rbfm.scan(fh,tablesDescriptor,"table-name",EQ_OP,tableName,attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        page += nullFieldLen;
        return *(unsigned *)page;
    }
    return -2;
}

string RelationManager::getFileName(const std::string &tableName) {
    FileHandle fh;
    int rc = rbfm.openFile("Tables",fh);
    if(rc != 0)
        return string();

    byte page[PAGE_SIZE];
    RID rid;
    RBFM_ScanIterator it = RBFM_ScanIterator();
    std::vector<string> attr = {"table-id","table-name","file-name"};
    rbfm.scan(fh,tablesDescriptor,"table-name",EQ_OP,tableName,attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        page += nullFieldLen+sizeof(unsigned);
        unsigned tableNameLen = *(unsigned *)page;
        page += sizeof(unsigned);
        res = string((char *)page,tableNameLen);
    }
    return res;
}

/**************************************
When inserting tuples to newly created catalog tables, we cannot call the default insertTuple method
 because the catalog tables don't yet contain any table nor column info. In another words, this function
 is to be called from createCatalog() method only.
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO INSERT: -3
**************************************/
RC RelationManager::insertCatalogTableTuple(const std::string &tableName, const std::vector<Attribute> &attrs, const void *data, RID &rid) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    rc = rbfm.insertRecord(fh,attrs,data,rid);
    if(rc < 0)
        return -3;

    return 0;
}

/* NOTE *************************************************************************************************************
createTableTableRow and createColumnTableRow methods transform rows to be inserted into Table/Column catalog tables
 into raw std::vector<byte>& bytesToWrite data. I could have supply a helper function that would append at the end of
 such vector either unsigned or string field, in order to minimalize the use of pointers, but didn't have time
 for this change right now, it's not that important
 ********************************************************************************************************************/
void RelationManager::createTableTableRow(const unsigned& tableID, const std::string& tableName, std::vector<byte>& bytesToWrite) {
    //Null field at the beginning
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(tablesDescriptor.size()/8.0));
    const byte* nullInfoFieldLengthPtr = reinterpret_cast<const byte*>(&nullInfoFieldLength);
    bytesToWrite.insert(bytesToWrite.end(), nullInfoFieldLengthPtr, nullInfoFieldLengthPtr + sizeof(nullInfoFieldLength));

    //Table ID
    const byte* lastTableIDPtr = reinterpret_cast<const byte*>(&tableID);
    bytesToWrite.insert(bytesToWrite.end(), lastTableIDPtr, lastTableIDPtr + sizeof(lastTableID));

    //Table name and filename
    unsigned tableNameLength = tableName.length();
    const byte* tableNameLengthPtr = reinterpret_cast<const byte*>(&tableNameLength);
    const byte* tableNamePtr = reinterpret_cast<const byte*>(tableName.c_str());

    for(int i = 0 ; i < 2 ; ++i) { //because in our implementation, tableName == tableFileName
        bytesToWrite.insert(bytesToWrite.end(), tableNameLengthPtr, tableNameLengthPtr + sizeof(tableNameLength));
        bytesToWrite.insert(bytesToWrite.end(), tableNamePtr, tableNamePtr + tableNameLength);
    }
}

void RelationManager::createColumnTableRow(const unsigned& tableID, const Attribute& attribute, const unsigned& colPos, std::vector<byte>& bytesToWrite) {
    //Null field at the beginning
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(numberOfColumnsTblFields/8.0));
    const byte* nullInfoFieldLengthPtr = reinterpret_cast<const byte*>(&nullInfoFieldLength);
    bytesToWrite.insert(bytesToWrite.end(), nullInfoFieldLengthPtr, nullInfoFieldLengthPtr + sizeof(nullInfoFieldLength));

    //Table ID
    const byte* lastTableIDPtr = reinterpret_cast<const byte*>(&tableID);
    bytesToWrite.insert(bytesToWrite.end(), lastTableIDPtr, lastTableIDPtr + sizeof(lastTableID));

    //Attribute name
    unsigned attrNameLen = attribute.name.length();
    const byte* attrNameLenPtr = reinterpret_cast<const byte*>(&attrNameLen);
    const byte* attrNamePtr = reinterpret_cast<const byte*>(attribute.name.c_str());
    bytesToWrite.insert(bytesToWrite.end(), attrNameLenPtr, attrNameLenPtr + sizeof(attrNameLen));
    bytesToWrite.insert(bytesToWrite.end(), attrNamePtr, attrNamePtr + attrNameLen);

    //Attribute type
    AttrType attrType = attribute.type;
    const byte* attrTypePtr = reinterpret_cast<const byte*>(&attrType);
    bytesToWrite.insert(bytesToWrite.end(), attrTypePtr, attrTypePtr + sizeof(AttrType));

    //Attribute length
    AttrLength attrLen = attribute.length;
    const byte* attrLenPtr = reinterpret_cast<const byte*>(&attrLen);
    bytesToWrite.insert(bytesToWrite.end(), attrLenPtr, attrLenPtr + sizeof(AttrLength));

    //Column position
    const byte* colPosPtr = reinterpret_cast<const byte*>(&colPos);
    bytesToWrite.insert(bytesToWrite.end(), colPosPtr, colPosPtr + sizeof(colPos));
}

RC RelationManager::createCatalog() {
    if(PagedFileManager::instance().createFile("Tables") != 0) {
        return -1;
    }
    if(PagedFileManager::instance().createFile("Columns") != 0) {
        return -1;
    }

    std::vector<byte> bytesToWrite;
    createTableTableRow(++lastTableID, "Tables", bytesToWrite);
    RID rid;
    RC rc = insertCatalogTableTuple("Tables", tablesDescriptor, bytesToWrite.data(), rid);
    if(rc != 0) {
        return  rc;
    }

    for(unsigned i = 0 ; i < columnDescriptor.size() ; ++i) {
        bytesToWrite.clear();
        createColumnTableRow(lastTableID, columnDescriptor[i], i+1, bytesToWrite);
        rc = insertCatalogTableTuple("Columns", columnDescriptor, bytesToWrite.data(), rid);
        if(rc != 0) {
            return  rc;
        }
    }
    return 0;
}

RC RelationManager::deleteCatalog() {
    if(PagedFileManager::instance().destroyFile("Tables") != 0) {
        return -1;
    }
    if(PagedFileManager::instance().destroyFile("Columns") != 0) {
        return -1;
    }
    return 0;
}

/* NOTE ***************************
 * In contrast to createCatalog(), this method calls insertTuple instead of insertCatalogTableTuple
 * in order to insert records to tables
 * ********************************/
RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    if(PagedFileManager::instance().createFile(tableName) != 0) {
        return -1;
    }

    std::vector<byte> bytesToWrite;
    createTableTableRow(++lastTableID, tableName, bytesToWrite);
    RID rid;
    RC rc = insertTuple("Tables", bytesToWrite.data(), rid);
    if(rc != 0) {
        return  rc;
    }

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        bytesToWrite.clear();
        createColumnTableRow(lastTableID, attrs[i], i+1, bytesToWrite);
        rc = insertTuple("Columns", bytesToWrite.data(), rid);
        if(rc != 0) {
            return  rc;
        }
    }
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return tableID;
    }

    std::vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    if(PagedFileManager::instance().destroyFile(tableName) != 0) {
        return -1;
    }

    RM_ScanIterator tableIt;
    std::vector<std::string> attributeNamesEmpty;   //we only want to get RID, not any fields
    scan("Tables", "table-id", CompOp::EQ_OP, &tableID, attributeNamesEmpty, tableIt);

    RID rid;
    void* dummyPtr = nullptr;   //we only want to get RID, not any fields
    rc = tableIt.getNextTuple(rid, dummyPtr);
    if(rc != 0) {
        return rc;
    }
    //We first delete one row corresponding to this table in the system catalog "Table"
    rc = deleteTuple("Tables", rid);
    if(rc != 0) {
        return rc;
    }

    //We then delete all of the rows corresponding to table's columns in the system catalog "Columns"
    scan("Columns", "table-id", CompOp::EQ_OP, &tableID, attributeNamesEmpty, tableIt);
    int counter = 0;
    for( ; counter < attrs.size() && (rc = tableIt.getNextTuple(rid, nullptr)) != RM_EOF ; ++counter) {
        deleteTuple("Columns", rid);
    }
    if(counter < attrs.size()) { //error occurred, other than RM_EOF
        return rc;
    }

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
NO FILE ASSOCIATED WITH TABLENAME: -2
SCAN ERROR: -3
**************************************/
RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    int id = getIdFromTableName(tableName);
    if(id < 0) return id;

    FileHandle fh;
    rbfm.openFile("Columns",fh);
    if(rc != 0)
        return -1;

    byte page[PAGE_SIZE];
    FileHandle fh;
    RID rid;
    string res;
    RBFM_ScanIterator it = RBFM_ScanIterator();
    std::vector<string> attr = {"table-id","column-name","column-type","column-length","column-position"};
    rc = rbfm.scan(fh,tablesDescriptor,"table-id",EQ_OP,&id,attr,it);
    if(rc != 0)
        return -3;

    while(it.getNextRecord(rid,page) != RBFM_EOF){
        byte *cur = page;
        Attribute tmp;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen+sizeof(unsigned);

        unsigned tableNameLen = *(unsigned *)cur;
        cur += sizeof(unsigned);
        string fieldName = string((char *)cur,tableNameLen);
        tmp.name = fieldName;

        tmp.type = *(unsigned *)cur;
        cur += sizeof(unsigned);

        tmp.length = *(unsigned *)cur;
        attrs.push_back(tmp);
    }

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO INSERT: -3
**************************************/
RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0)
        return -2;

    rc = rbfm.insertRecord(fh,attrs,data,rid);
    if(rc < 0)
        return -3;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO DELETE: -3
**************************************/
RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0)
        return -2;

    rc = rbfm.deleteRecord(fh,attrs,rid);
    if(rc < 0)
        return -3;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO UPDATE: -3
**************************************/
RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0)
        return -2;

    rc = rbfm.updateRecord(fh,attrs,data,rid);
    if(rc < 0)
        return -3;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO READ: -3
**************************************/
RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0)
        return -2;

    rc = rbfm.readRecord(fh,attrs,rid,data);
    if(rc < 0)
        return -3;

    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return RecordBasedFileManager::instance().printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    FileHandle fh;
    RC rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    return RecordBasedFileManager::instance().readAttribute(fh, attrs, rid, attributeName, data);
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    FileHandle fh;
    RC rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    return RecordBasedFileManager::instance().scan(fh, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.getRbfmIt());
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}



