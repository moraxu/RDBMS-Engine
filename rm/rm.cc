#include "rm.h"

using namespace std;

RelationManager *RelationManager::_relation_manager = nullptr;

//Change return type to RelationManager *
RelationManager &RelationManager::instance() {
    static _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager(){
    rbfm = RecordBasedFileManager();
    createTableDescriptor();
    createColumnDescriptor();
}

RelationManager::~RelationManager() { delete _relation_manager; }

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
        tablesDescriptor.push_back(tmp);
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

RC RelationManager::closeFile(FileHandle &fileHandle){
    return rbfm.closeFile(fileHandle);
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FILE CLOSE ERROR: -2
NO FILE ASSOCIATED WITH TABLENAME: -3
**************************************/
RC RelationManager::getIdFromTableName(const std::string &tableName){
    FileHandle fh;
    int rc = rbfm.openFile("Tables",fh);
    if(rc != 0)
        return -1;

    byte page[PAGE_SIZE];
    RID rid;
    string res;
    RBFM_ScanIterator it = RBFM_ScanIterator();
    std::vector<string> attr = {"table-id","table-name","file-name"};
    rbfm.scan(fh,tablesDescriptor,"table-name",EQ_OP,tableName.c_str(),attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        page += nullFieldLen;
        return *(unsigned *)page;
    }
    it.close();
    rc = closeFile(fh);
    if(rc != 0)
        return -2;

    return -3;
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
    rbfm.scan(fh,tablesDescriptor,"table-name",EQ_OP,tableName.c_str(),attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        page += nullFieldLen+sizeof(unsigned);
        unsigned tableNameLen = *(unsigned *)page;
        page += sizeof(unsigned);
        res = string((char *)page,tableNameLen);
    }
    it.close();

    rc = closeFile(fh);
    //if(rc != 0)
    //   return string();
    return res;
}

RC RelationManager::createCatalog() {
    return 0;
}

RC RelationManager::createCatalog() {
    return -1;
}

RC RelationManager::deleteCatalog() {
    return -1;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    return -1;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    return -1;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
NO FILE ASSOCIATED WITH TABLENAME: -2
SCAN ERROR: -3
FILE CLOSE ERROR: -4
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
    it.close();
    rc = closeFile(fh);
    if(rc != 0)
        return -4;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO INSERT: -3
FILE CLOSE ERROR: -4
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

    rc = closeFile(fh);
    if(rc != 0)
        return -4;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO DELETE: -3
FILE CLOSE ERROR: -4
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

    rc = closeFile(fh);
    if(rc != 0)
        return -4;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO UPDATE: -3
FILE CLOSE ERROR: -4
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

    rc = closeFile(fh);
    if(rc != 0)
        return -4;

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO GET ATTRIBUTE: -2
FAIL TO READ: -3
FILE CLOSE ERROR: -4
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

    rc = closeFile(fh);
    if(rc != 0)
        return -4;

    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return -1;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    return -1;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}



