#include "rm.h"
#include <cmath>
#include <iostream>

using namespace std;

//Change return type to RelationManager *
RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager(){
    createTableDescriptor();
    createColumnDescriptor();
    lastTableID = 0;
    numberOfColumnsTblFields = 5;
}

RelationManager::~RelationManager() {
    deleteCatalog();
}

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

/******************************************************************************************************
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
******************************************************************************************************/
RC RelationManager::createTableDescriptor(){
    vector<string> fieldName = {"table-id","table-name","file-name"};
    vector<AttrType> type = {TypeInt,TypeVarChar,TypeVarChar};
    vector<AttrLength> len = {sizeof(int),50,50};

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
    vector<AttrType> type = {TypeInt,TypeVarChar,TypeInt,TypeInt,TypeInt};
    vector<AttrLength> len = {sizeof(int),50,sizeof(int),sizeof(int),sizeof(int)};

    for(int i = 0;i < 5;i++){
        Attribute tmp;
        tmp.name = fieldName[i];
        tmp.type = type[i];
        tmp.length = len[i];
        columnDescriptor.push_back(tmp);
    }

    return 0;
}

std::string RelationManager::getFileName(const std::string &tableName) {
    FileHandle fh;
    string res;
    int rc = RecordBasedFileManager::instance().openFile("Tables",fh);
    if(rc != 0)
        return string();

    byte page[PAGE_SIZE];
    RID rid;

    //Filter the record based on comparison over "table-name" field, then extract "file-name" field from these filtered record
    RM_ScanIterator tableIt;
    std::vector<string> attr = {"file-name"};
    scan("Tables", "table-name", CompOp::EQ_OP, tableName.c_str(), attr, tableIt);

    //Assume the length of content to be extracted is less than PAGE_SIZE
    if(tableIt.getNextTuple(rid,page) != RBFM_EOF){
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;
        unsigned fileNameLen = *(unsigned *)cur;
        cur += sizeof(unsigned);
        res = string((char *)cur,fileNameLen);
    }
    rc = tableIt.close();
    if(rc != 0)
       return string();
    return res;
}

RC RelationManager::openFile(const std::string &tableName,FileHandle &fileHandle){
    string fn = getFileName(tableName);
    //No such file associated with tableName
    if(fn.empty())
        return -1;

    return RecordBasedFileManager::instance() .openFile(fn,fileHandle);
}

RC RelationManager::createFile(const std::string &fileName){
    return RecordBasedFileManager::instance().createFile(fileName);
}

RC RelationManager::closeFile(FileHandle &fileHandle){
    return RecordBasedFileManager::instance().closeFile(fileHandle);
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FILE CLOSE ERROR: -2
NO FILE ASSOCIATED WITH TABLENAME: -3
**************************************/
int RelationManager::getIdFromTableName(const std::string &tableName){
    cout<<"In getIdFromTableName:"<<endl;
    FileHandle fh;
    int res = -3,cnt = 1;
    int rc = RecordBasedFileManager::instance().openFile("Tables",fh);
    if(rc != 0)
        return -1;
    cout<<cnt++<<endl;

    byte page[sizeof(unsigned)+1];
    RID rid;
    RBFM_ScanIterator it;
    std::vector<string> attr = {"table-id"};
    RecordBasedFileManager::instance().scan(fh,tablesDescriptor,"table-name",EQ_OP,tableName.c_str(),attr,it);
    cout<<cnt++<<endl;

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        cout<<cnt++<<" "<<res<<endl;
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;
        res = *(unsigned *)cur;
        cout<<cnt++<<" "<<res<<endl;
    }
    rc = it.close();
    if(rc != 0)
        return -2;

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
    int rc = RecordBasedFileManager::instance().openFile(tableName,fh);
    if(rc != 0)
        return -1;

    rc = RecordBasedFileManager::instance().insertRecord(fh,attrs,data,rid);
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
    vector<byte> emptyBytes(nullInfoFieldLength);
    bytesToWrite.insert(bytesToWrite.end(), emptyBytes.begin(), emptyBytes.end());

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
    vector<byte> emptyBytes(nullInfoFieldLength);
    bytesToWrite.insert(bytesToWrite.end(), emptyBytes.begin(), emptyBytes.end());

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

    return createTableHelper("Tables", columnDescriptor);
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

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    if(PagedFileManager::instance().createFile(tableName) != 0) {
        return -1;
    }

    return createTableHelper(tableName, attrs);
}

RC RelationManager::createTableHelper(const std::string &tableName, const std::vector<Attribute> &attrs) {
    std::vector<byte> bytesToWrite;
    createTableTableRow(++lastTableID, tableName, bytesToWrite);
    RID rid;
    RC rc = insertCatalogTableTuple("Tables", tablesDescriptor, bytesToWrite.data(), rid);
    if(rc != 0) {
        return  rc;
    }

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        bytesToWrite.clear();
        createColumnTableRow(lastTableID, attrs[i], i+1, bytesToWrite);
        rc = insertCatalogTableTuple("Columns", columnDescriptor, bytesToWrite.data(), rid);
        if(rc != 0) {
            return  rc;
        }
    }
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    int tableID = getIdFromTableName(tableName);
    cout << "tableID = " << tableID << "\n";
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

    RM_ScanIterator columnIt;
    //We then delete all of the rows corresponding to table's columns in the system catalog "Columns"
    scan("Columns", "table-id", CompOp::EQ_OP, &tableID, attributeNamesEmpty, columnIt);
    int counter = 0;
    for( ; counter < attrs.size() && (rc = columnIt.getNextTuple(rid, nullptr)) != RM_EOF ; ++counter) {
        deleteTuple("Columns", rid);
    }
    //Iterator needs to be closed
    tableIt.close();
    columnIt.close();

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
FILE CLOSE ERROR: -4

NOTE: 'attrs' could be empty after this method returns 0!
**************************************/
RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    cout<<"In getAttributes:"<<endl;
    int cnt = 1;
    int id = getIdFromTableName(tableName);
    if(id < 0) return id;
    cout<<cnt++<<endl;

    FileHandle fh;
    int rc = RecordBasedFileManager::instance().openFile("Columns",fh);
    if(rc != 0)
        return -1;
    cout<<cnt++<<endl;

    byte page[PAGE_SIZE];
    RID rid;
    RBFM_ScanIterator it;
    std::vector<string> attr = {"column-name","column-type","column-length","column-position"};
    RecordBasedFileManager::instance().scan(fh,columnDescriptor,"table-id", CompOp::EQ_OP, &id, attr, it);
    cout<<cnt++<<endl;

    //Assume the length of content to be extracted is less than PAGE_SIZE
    while(it.getNextRecord(rid,page) != RBFM_EOF){
        byte *cur = page;
        Attribute tmp;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;

        unsigned columnNameLen = *(unsigned *)cur;
        cur += sizeof(unsigned);
        string columnName = string((char *)cur,columnNameLen);
        tmp.name = columnName;
        cur += columnNameLen;

        tmp.type = *(AttrType *)cur;
        cur += sizeof(unsigned);

        tmp.length = *(AttrLength *)cur;
        attrs.push_back(tmp);
        cout<<cnt++<<" "<<tmp.name<<" "<<tmp.type<<" "<<tmp.length<<endl;
    }

    rc = it.close();
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

    rc = RecordBasedFileManager::instance().insertRecord(fh,attrs,data,rid);
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

    rc = RecordBasedFileManager::instance().deleteRecord(fh,attrs,rid);
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

    rc = RecordBasedFileManager::instance().updateRecord(fh,attrs,data,rid);
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

    rc = RecordBasedFileManager::instance().readRecord(fh,attrs,rid,data);
    if(rc < 0)
        return -3;

    rc = closeFile(fh);
    if(rc != 0)
        return -4;

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
    int cnt = 1;
    RC rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }
    cout<<cnt<<endl;

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



