#include "rm.h"
#include "../ix/ix.h"
#include <cmath>
#include <iostream>

using namespace std;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager(){
    createTableDescriptor();
    createColumnDescriptor();
    createIndexDescriptor();
    lastTableID = 0;
    FileHandle fH;
    if(openFile("Tables", fH) == 0) {
        lastTableID = fH.getLastTableId();
        closeFile(fH);
    }
    modifySystemTable_AdminRequest = false;
}

RelationManager::~RelationManager() {

}

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

/******************************************************************************************************
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), system-table:int
******************************************************************************************************/
RC RelationManager::createTableDescriptor(){
    vector<string> fieldName = {"table-id","table-name","file-name","system-table"};
    vector<AttrType> type = {TypeInt,TypeVarChar,TypeVarChar,TypeInt};
    vector<AttrLength> len = {sizeof(int),50,50,sizeof(int)};

    for(int i = 0;i < fieldName.size();i++){
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

    for(int i = 0;i < fieldName.size();i++){
        Attribute tmp;
        tmp.name = fieldName[i];
        tmp.type = type[i];
        tmp.length = len[i];
        columnDescriptor.push_back(tmp);
    }

    return 0;
}

/******************************************************************************************************
Indexes(table-id:int, attribute-name:varchar(50), file-name:varchar(50))
******************************************************************************************************/
RC RelationManager::createIndexDescriptor(){
    vector<string> fieldName = {"table-id","attribute-name","file-name"};
    vector<AttrType> type = {TypeInt,TypeVarChar,TypeVarChar};
    vector<AttrLength> len = {sizeof(int),50,50};

    for(int i = 0;i < fieldName.size();i++){
        Attribute tmp;
        tmp.name = fieldName[i];
        tmp.type = type[i];
        tmp.length = len[i];
        indexesDescriptor.push_back(tmp);
    }

    return 0;
}

std::string RelationManager::getTableFilename(const std::string &tableName) {
    string res;
    byte page[PAGE_SIZE];
    RID rid;

    vector<byte> stringValue;
    unsigned stringLen = tableName.length();
    byte* stringLenPtr = reinterpret_cast<byte*>(&stringLen);
    const byte* stringContPtr = reinterpret_cast<const byte*>(tableName.data());
    stringValue.insert(stringValue.end(), stringLenPtr, stringLenPtr+sizeof(unsigned));
    stringValue.insert(stringValue.end(), stringContPtr, stringContPtr+stringLen);

    //Filter the record based on comparison over "table-name" field, then extract "file-name" field from these filtered record
    RM_ScanIterator tableIt;
    std::vector<string> attr = {"file-name"};
    scan("Tables", "table-name", CompOp::EQ_OP, stringValue.data(), attr, tableIt);

    //Assume the length of content to be extracted is less than PAGE_SIZE
    if(tableIt.getNextTuple(rid,page) != RBFM_EOF){
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;
        unsigned fileNameLen = *(unsigned *)cur;
        cur += sizeof(unsigned);
        res = string((char *)cur,fileNameLen);
    }
    int rc = tableIt.close();
    if(rc != 0)
        return string();
    return res;
}

RC RelationManager::openFile(const std::string &tableName,FileHandle &fileHandle){
    string fn;
    if(tableName == "Tables" || tableName == "Columns" || tableName == "Indexes") fn = tableName;
    else fn = getTableFilename(tableName);
    //No such file associated with tableName
    if(fn.empty())
        return -1;

    return RecordBasedFileManager::instance().openFile(fn,fileHandle);
}

RC RelationManager::createFile(const std::string &fileName){
    return RecordBasedFileManager::instance().createFile(fileName);
}

RC RelationManager::closeFile(FileHandle &fileHandle){
    return RecordBasedFileManager::instance().closeFile(fileHandle);
}

/**************************************
WHEN return value < 1:
INEXISTENT TABLE ID: 0 (because numbering starts from 1)
FILE OPEN ERROR: -1
FILE CLOSE ERROR: -2
NO FILE ASSOCIATED WITH TABLENAME: -3
**************************************/
int RelationManager::getIdFromTableName(const std::string &tableName){
    FileHandle fh;
    int res = -3;

    int rc = RecordBasedFileManager::instance().openFile("Tables",fh);
    if(rc != 0) {
        return -1;
    }

    byte page[sizeof(unsigned)+1];
    RID rid;

    vector<byte> stringValue;
    unsigned stringLen = tableName.length();
    byte* stringLenPtr = reinterpret_cast<byte*>(&stringLen);
    const byte* stringContPtr = reinterpret_cast<const byte*>(tableName.data());
    stringValue.insert(stringValue.end(), stringLenPtr, stringLenPtr+sizeof(unsigned));
    stringValue.insert(stringValue.end(), stringContPtr, stringContPtr+stringLen);

    RBFM_ScanIterator it;
    std::vector<string> attr = {"table-id"};
    RecordBasedFileManager::instance().scan(fh,tablesDescriptor,"table-name",EQ_OP,stringValue.data(),attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;
        res = *(unsigned *)cur;
    }
    rc = it.close();
    if(rc != 0) {
        return -2;
    }

    return res;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
FAIL TO INSERT: -3
**************************************/
RC RelationManager::insertCatalogTableTuple(const std::string &tableName, const std::vector<Attribute> &attrs, const void *data, RID &rid) {
    FileHandle fh;
    int rc = openFile(tableName,fh);
    if(rc != 0)
        return -1;

    rc = RecordBasedFileManager::instance().insertRecord(fh,attrs,data,rid);
    if(rc < 0) {
        closeFile(fh);
        return -3;
    }

    return closeFile(fh);
}

/* NOTE *************************************************************************************************************
 createTableTableRow, createColumnTableRow and createIndexTableRow methods transform rows to be inserted into
 Table/Column/Index catalog tables into raw std::vector<byte>& bytesToWrite data. I could have supply a helper
 function that would append at the end of such vector either unsigned or string field, in order to minimalize
 the use of pointers, but didn't have time for this change right now, it's not that important
 ********************************************************************************************************************/
void RelationManager::createTableTableRow(const unsigned& tableID, const std::string& tableName, std::vector<byte>& bytesToWrite, bool systemTable) {
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

    //Is system table
    unsigned isSystemTable = systemTable ? 1 : 0;
    const byte* isSystemTablePtr = reinterpret_cast<const byte*>(&isSystemTable);
    bytesToWrite.insert(bytesToWrite.end(), isSystemTablePtr, isSystemTablePtr + sizeof(isSystemTable));
}

void RelationManager::createColumnTableRow(const unsigned& tableID, const Attribute& attribute, const unsigned& colPos, std::vector<byte>& bytesToWrite) {
    //Null field at the beginning
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(columnDescriptor.size()/8.0));
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
void RelationManager::createIndexTableRow(const unsigned& tableID, const std::string& attributeName, const std::string& filename, std::vector<byte>& bytesToWrite) {
    //Null field at the beginning
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(indexesDescriptor.size()/8.0));
    vector<byte> emptyBytes(nullInfoFieldLength);
    bytesToWrite.insert(bytesToWrite.end(), emptyBytes.begin(), emptyBytes.end());

    //Table ID
    const byte* tableIDPtr = reinterpret_cast<const byte*>(&tableID);
    bytesToWrite.insert(bytesToWrite.end(), tableIDPtr, tableIDPtr + sizeof(lastTableID));

    //Attribute name + filename
    const std::string names[2] = { attributeName, filename };
    for(int i = 0 ; i < 2 ; ++i) {
        unsigned nameLength = names[i].length();
        const byte* nameLengthPtr = reinterpret_cast<const byte*>(&nameLength);
        const byte* namePtr = reinterpret_cast<const byte*>(names[i].c_str());

        bytesToWrite.insert(bytesToWrite.end(), nameLengthPtr, nameLengthPtr + sizeof(nameLength));
        bytesToWrite.insert(bytesToWrite.end(), namePtr, namePtr + nameLength);
    }
}

RC RelationManager::createCatalog() {
    if(PagedFileManager::instance().createFile("Tables") != 0) {
        return -1;
    }
    if(RecordBasedFileManager::instance().createFile("Columns") != 0) {
        return -1;
    }
    if(RecordBasedFileManager::instance().createFile("Indexes") != 0) {
        return -1;
    }

    RC rc = createTableHelper("Tables", tablesDescriptor, true);
    if(rc != 0) {
        return  rc;
    }

    rc = createTableHelper("Columns", columnDescriptor,true);
    if(rc != 0) {
        return  rc;
    }

    rc = createTableHelper("Indexes", indexesDescriptor,true);
    if(rc != 0) {
        return  rc;
    }

    FileHandle fH;
    rc = openFile("Tables", fH);
    if(rc != 0) {
        return  rc;
    }
    fH.setLastTableId(lastTableID);
    return closeFile(fH);
}

RC RelationManager::deleteCatalog() {
    if(RecordBasedFileManager::instance().destroyFile("Tables") != 0) {
        return -1;
    }
    if(RecordBasedFileManager::instance().destroyFile("Columns") != 0) {
        return -1;
    }
    if(RecordBasedFileManager::instance().destroyFile("Indexes") != 0) {
        return -1;
    }
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    if(PagedFileManager::instance().createFile(tableName) != 0) {
        return -1;
    }

    RC rc = createTableHelper(tableName, attrs, false);
    if(rc != 0) {
        return  rc;
    }

    FileHandle fH;
    rc = openFile("Tables", fH);
    if(rc != 0) {
        return  rc;
    }
    fH.setLastTableId(lastTableID);
    return closeFile(fH);
}

RC RelationManager::createTableHelper(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystemTable) {
    std::vector<byte> bytesToWrite;
    createTableTableRow(++lastTableID, tableName, bytesToWrite, isSystemTable);
    RID dummyRID; //dummy RID, we don't need it
    RC rc = insertCatalogTableTuple("Tables", tablesDescriptor, bytesToWrite.data(), dummyRID);
    if(rc != 0) {
        return  rc;
    }

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        bytesToWrite.clear();
        createColumnTableRow(lastTableID, attrs[i], i+1, bytesToWrite);
        rc = insertCatalogTableTuple("Columns", columnDescriptor, bytesToWrite.data(), dummyRID);
        if(rc != 0) {
            return  rc;
        }
    }
    return 0;
}

RC RelationManager::isSystemTable(const std::string& tableName, bool& isSysTable) {
    FileHandle fh;
    RC rc = RecordBasedFileManager::instance().openFile("Tables",fh);
    if(rc != 0) {
        return -1;
    }

    vector<byte> stringValue;
    unsigned stringLen = tableName.length();
    byte* stringLenPtr = reinterpret_cast<byte*>(&stringLen);
    const byte* stringContPtr = reinterpret_cast<const byte*>(tableName.data());
    stringValue.insert(stringValue.end(), stringLenPtr, stringLenPtr+sizeof(unsigned));
    stringValue.insert(stringValue.end(), stringContPtr, stringContPtr+stringLen);

    byte page[sizeof(unsigned)+1];
    RID rid;
    RBFM_ScanIterator it;
    std::vector<string> attr = {"system-table"};
    RecordBasedFileManager::instance().scan(fh,tablesDescriptor,"table-name",EQ_OP,stringValue.data(),attr,it);

    if(it.getNextRecord(rid,page) != RBFM_EOF){
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;
        isSysTable = *(unsigned *)cur == 1 ? true : false;
    }
    return it.close();
}

RC RelationManager::deleteTable(const std::string &tableName) {
    RC rc;
    if(!modifySystemTable_AdminRequest) {
        bool isSysTable;
        rc = isSystemTable(tableName, isSysTable);
        if(rc != 0) {
            return rc;
        }
        if(isSysTable) {
            return -1;
        }
    }

    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    std::set<std::string> attributeNames;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        attributeNames.insert(attrs[i].name);
    }
    if(destroyIndexes(tableID, attributeNames) == -2) {
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
        tableIt.close();
        return rc;
    }

    //--------Beginning of the section where we use deleteTuple for admin's purposes
    //Therefore it's okey in here to call deleteTuple to modify system tables
    modifySystemTable_AdminRequest = true;
    //We first delete one row corresponding to this table in the system catalog "Table"
    rc = deleteTuple("Tables", rid);
    if(rc != 0) {
        tableIt.close();
        return rc;
    }

    RM_ScanIterator columnIt;
    //We then delete all of the rows corresponding to table's columns in the system catalog "Columns"
    scan("Columns", "table-id", CompOp::EQ_OP, &tableID, attributeNamesEmpty, columnIt);
    int counter = 0;
    for( ; counter < attrs.size() && (rc = columnIt.getNextTuple(rid, nullptr)) != RM_EOF ; ++counter) {
        rc = deleteTuple("Columns", rid);
        if(rc != 0) {
            tableIt.close();
            columnIt.close();
            return rc;
        }
    }
    modifySystemTable_AdminRequest = false;
    //--------End of the section where we used deleteTuple for admin's purposes

    //Iterator needs to be closed
    rc = tableIt.close();
    if(rc != 0) {
        columnIt.close();
        return rc;
    }
    rc = columnIt.close();
    if(rc != 0) {
        return rc;
    }

    if(counter < attrs.size()) { //error occurred, other than RM_EOF
        return -1;
    }

    return 0;
}

/**************************************
WHEN return value < 0:
FILE OPEN ERROR: -1
NO FILE ASSOCIATED WITH TABLENAME: -2
SCAN ERROR: -3
FILE CLOSE ERROR: -4
 FAILED TO GET TABLE ID: -5

NOTE: 'attrs' could be empty after this method returns 0!
**************************************/
RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    if(tableName == string("Tables")) {
        attrs = tablesDescriptor;
        return 0;
    }
    if(tableName == string("Columns")){
        attrs = columnDescriptor;
        return 0;
    }
    if(tableName == string("Indexes")){
        attrs = indexesDescriptor;
        return 0;
    }

    int id = getIdFromTableName(tableName);
    if(id < 1) {
        return -5;
    }

    FileHandle fh;
    int rc = RecordBasedFileManager::instance().openFile("Columns",fh);
    if(rc != 0) {
        return -1;
    }

    byte page[PAGE_SIZE];
    RID rid;
    RBFM_ScanIterator it;
    std::vector<string> attr = {"column-name","column-type","column-length","column-position"};
    RecordBasedFileManager::instance().scan(fh,columnDescriptor,"table-id", CompOp::EQ_OP, &id, attr, it);

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
    }

    rc = it.close();
    if(rc != 0) {
        return -4;
    }

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
    RC rc;
    if(!modifySystemTable_AdminRequest) {
        bool isSysTable;
        rc = isSystemTable(tableName, isSysTable);
        if(rc != 0) {
            return rc;
        }
        if(isSysTable) {
            return -1;
        }
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0) {
        return -2;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    rc = RecordBasedFileManager::instance().insertRecord(fh,attrs,data,rid);
    if(rc < 0) {
        closeFile(fh);
        return -3;
    }

    rc = closeFile(fh);
    if(rc != 0) {
        return -4;
    }

    return handleIndexesForInsertion(tableName, attrs, data, rid);
}

RC RelationManager::handleIndexesForInsertion(const std::string &tableName, const std::vector<Attribute> &attrs, const void *data, const RID &rid) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }
    std::set<std::string> attributeNames;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        attributeNames.insert(attrs[i].name);
    }
    map<string, IndexAttributeInfo> indexAttributes;
    if(processIndexesForTable(tableID, attributeNames, indexAttributes) != 0) {
        return -1;
    }

    //Once we have "attributes" map initialized with keys being attribute names for which indexes exist,
    //we iterate through fields to check if an index is created on some of them
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(attrs.size()/8.0));
    const byte* actualData = reinterpret_cast<const byte*>(data) + nullInfoFieldLength;

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        const byte *byteInNullInfoField = reinterpret_cast<const byte *>(data) + i / 8;

        bool nullField = *byteInNullInfoField & (1 << 7 - i % 8);
        if (!nullField && indexAttributes.find(attrs[i].name) != indexAttributes.end()) {
            IXFileHandle ixFileHandle;
            RC rc = IndexManager::instance().openFile(indexAttributes[attrs[i].name].filename, ixFileHandle);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().insertEntry(ixFileHandle, attrs[i], actualData, rid);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().closeFile(ixFileHandle);
            if(rc != 0) {
                return -1;
            }

            if (attrs[i].type == AttrType::TypeInt || attrs[i].type == AttrType::TypeReal) {
                actualData += attrs[i].length;
            } else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned *>(actualData);
                actualData += (4 + varCharLength);
            }
        }
    }

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
    RC rc;
    if(!modifySystemTable_AdminRequest) {
        bool isSysTable;
        rc = isSystemTable(tableName, isSysTable);
        if(rc != 0) {
            return rc;
        }
        if(isSysTable) {
            return -1;
        }
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0) {
        return -2;
    }

    rc = handleIndexesForDeletion(tableName, attrs, rid);
    if(rc != 0) {
        return -1;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    rc = RecordBasedFileManager::instance().deleteRecord(fh,attrs,rid);
    if(rc < 0) {
        closeFile(fh);
        return -3;
    }

    rc = closeFile(fh);
    if(rc != 0) {
        return -4;
    }

    return 0;
}

RC RelationManager::handleIndexesForDeletion(const std::string &tableName, const std::vector<Attribute> &attrs, const RID &rid) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }
    std::set<std::string> attributeNames;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        attributeNames.insert(attrs[i].name);
    }
    map<string, IndexAttributeInfo> indexAttributes;
    if(processIndexesForTable(tableID, attributeNames, indexAttributes) != 0) {
        return -1;
    }

    byte data[PAGE_SIZE];
    RC rc = readTuple(tableName, rid, data);
    if(rc != 0) {
        return -1;
    }
    //Once we have "attributes" map initialized with keys being attribute names for which indexes exist,
    //we iterate through fields to check if an index is created on some of them
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(attrs.size()/8.0));
    const byte* actualData = reinterpret_cast<const byte*>(data) + nullInfoFieldLength;

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        const byte *byteInNullInfoField = reinterpret_cast<const byte *>(data) + i / 8;

        bool nullField = *byteInNullInfoField & (1 << 7 - i % 8);
        if (!nullField && indexAttributes.find(attrs[i].name) != indexAttributes.end()) {
            IXFileHandle ixFileHandle;
            rc = IndexManager::instance().openFile(indexAttributes[attrs[i].name].filename, ixFileHandle);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().deleteEntry(ixFileHandle, attrs[i], actualData, rid);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().closeFile(ixFileHandle);
            if(rc != 0) {
                return -1;
            }

            if (attrs[i].type == AttrType::TypeInt || attrs[i].type == AttrType::TypeReal) {
                actualData += attrs[i].length;
            } else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned *>(actualData);
                actualData += (4 + varCharLength);
            }
        }
    }

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
    RC rc;
    if(!modifySystemTable_AdminRequest) {
        bool isSysTable;
        rc = isSystemTable(tableName, isSysTable);
        if(rc != 0) {
            return rc;
        }
        if(isSysTable) {
            return -1;
        }
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName,attrs);
    if(rc < 0) {
        return -2;
    }

    rc = handleIndexesForUpdate(tableName, attrs, data, rid);
    if(rc != 0) {
        return -1;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    rc = RecordBasedFileManager::instance().updateRecord(fh,attrs,data,rid);
    if(rc < 0) {
        closeFile(fh);
        return -3;
    }

    rc = closeFile(fh);
    if(rc != 0) {
        return -4;
    }

    return 0;
}

RC RelationManager::handleIndexesForUpdate(const std::string &tableName, const std::vector<Attribute> &attrs, const void *data, const RID &rid) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }
    std::set<std::string> attributeNames;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        attributeNames.insert(attrs[i].name);
    }
    map<string, IndexAttributeInfo> indexAttributes;
    if(processIndexesForTable(tableID, attributeNames, indexAttributes) != 0) {
        return -1;
    }

    byte previousData[PAGE_SIZE];
    RC rc = readTuple(tableName, rid, previousData);
    if(rc != 0) {
        return -1;
    }
    //Once we have "attributes" map initialized with keys being attribute names for which indexes exist,
    //we iterate through fields to check if an index is created on some of them
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(attrs.size()/8.0));
    const byte* actualData = reinterpret_cast<const byte*>(previousData) + nullInfoFieldLength;

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        const byte *byteInNullInfoField = reinterpret_cast<const byte *>(previousData) + i / 8;

        bool nullField = *byteInNullInfoField & (1 << 7 - i % 8);
        if (!nullField && indexAttributes.find(attrs[i].name) != indexAttributes.end()) {
            IXFileHandle ixFileHandle;
            rc = IndexManager::instance().openFile(indexAttributes[attrs[i].name].filename, ixFileHandle);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().deleteEntry(ixFileHandle, attrs[i], actualData, rid);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().closeFile(ixFileHandle);
            if(rc != 0) {
                return -1;
            }

            if (attrs[i].type == AttrType::TypeInt || attrs[i].type == AttrType::TypeReal) {
                actualData += attrs[i].length;
            } else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned *>(actualData);
                actualData += (4 + varCharLength);
            }
        }
    }

    //And now update part:
    actualData = reinterpret_cast<const byte*>(data) + nullInfoFieldLength;

    for(unsigned i = 0 ; i < attrs.size() ; ++i) {
        const byte *byteInNullInfoField = reinterpret_cast<const byte *>(data) + i / 8;

        bool nullField = *byteInNullInfoField & (1 << 7 - i % 8);
        if (!nullField && indexAttributes.find(attrs[i].name) != indexAttributes.end()) {
            IXFileHandle ixFileHandle;
            rc = IndexManager::instance().openFile(indexAttributes[attrs[i].name].filename, ixFileHandle);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().insertEntry(ixFileHandle, attrs[i], actualData, rid);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                return -1;
            }
            rc = IndexManager::instance().closeFile(ixFileHandle);
            if(rc != 0) {
                return -1;
            }

            if (attrs[i].type == AttrType::TypeInt || attrs[i].type == AttrType::TypeReal) {
                actualData += attrs[i].length;
            } else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned *>(actualData);
                actualData += (4 + varCharLength);
            }
        }
    }

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
    std::vector<Attribute> attrs;
    RC rc = getAttributes(tableName,attrs);
    if(rc < 0) {
        return -2;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    rc = RecordBasedFileManager::instance().readRecord(fh,attrs,rid,data);
    if(rc != 0) {
        closeFile(fh);
        return -3;
    }

    rc = closeFile(fh);
    if(rc != 0) {
        return -5;
    }
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return RecordBasedFileManager::instance().printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    std::vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    rc = RecordBasedFileManager::instance().readAttribute(fh, attrs, rid, attributeName, data);
    if(rc != 0) {
        closeFile(fh);
        return rc;
    }

    return closeFile(fh);
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    std::vector<Attribute> attrs;
    RC rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }

    FileHandle fh;
    rc = openFile(tableName,fh);
    if(rc != 0) {
        return -1;
    }

    return RecordBasedFileManager::instance().scan(fh, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.getRbfmIt());
}

RC RelationManager::processIndexesForTable(const unsigned &tableID, const std::set<std::string> &attributeNames, map<string, IndexAttributeInfo>& attributes) {
    FileHandle fh;
    RC rc = RecordBasedFileManager::instance().openFile("Indexes",fh);
    if(rc != 0) {
        return -1;
    }

    byte page[PAGE_SIZE];
    RID rid;
    RBFM_ScanIterator it;
    std::vector<string> attr = {"attribute-name","file-name"};
    RecordBasedFileManager::instance().scan(fh,indexesDescriptor,"table-id", CompOp::EQ_OP, &tableID, attr, it);

    while(it.getNextRecord(rid,page) != RBFM_EOF){
        byte *cur = page;
        unsigned nullFieldLen = ceil(attr.size()/8.0);
        cur += nullFieldLen;

        unsigned attributeNameLen = *(unsigned *)cur;
        cur += sizeof(unsigned);
        string readAttributeName = string((char *)cur,attributeNameLen);
        if(attributeNames.find(readAttributeName) != attributeNames.end()) {
            cur += attributeNameLen;

            unsigned fileNameLen = *(unsigned *)cur;
            cur += sizeof(unsigned);
            string fileName = string((char *)cur,fileNameLen);

            attributes[readAttributeName] = IndexAttributeInfo{rid, fileName};
        }
    }

    return it.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }

    //Our index files will have the following names: TableName_AttributeName
    const string indexFileName = tableName + "_" + attributeName;
    if(IndexManager::instance().createFile(indexFileName) != 0) {
        return -1;
    }

    std::vector<byte> bytesToWrite;
    createIndexTableRow(tableID, attributeName, indexFileName, bytesToWrite);
    RID dummyRID; //dummy RID, we don't need it
    RC rc = insertCatalogTableTuple("Indexes", indexesDescriptor, bytesToWrite.data(), dummyRID);
    if(rc != 0) {
        return -1;
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }
    Attribute attr;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        if(attrs[i].name == attributeName) {
            attr = attrs[i];
        }
    }

    //Populate index based on existing records in the given table
    byte page[PAGE_SIZE];
    RID rid;
    RM_ScanIterator tableIt;
    std::vector<string> projectedAttr = {attributeName};
    scan(tableName, "", NO_OP, NULL, projectedAttr, tableIt);

    if(tableIt.getNextTuple(rid,page) != RBFM_EOF){
        byte *cur = page;
        if(*cur) {  //NO_OP returns also nulls so have to test for them
            unsigned nullFieldLen = ceil(projectedAttr.size()/8.0);
            cur += nullFieldLen;

            IXFileHandle ixFileHandle;
            rc = IndexManager::instance().openFile(indexFileName, ixFileHandle);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                tableIt.close();
                return -1;
            }
            rc = IndexManager::instance().insertEntry(ixFileHandle, attr, cur, rid);
            if(rc != 0) {
                IndexManager::instance().closeFile(ixFileHandle);
                tableIt.close();
                return -1;
            }
            rc = IndexManager::instance().closeFile(ixFileHandle);
            if(rc != 0) {
                tableIt.close();
                return -1;
            }
        }
    }

    return tableIt.close();
}

/*
 * RETURN CODES:
 * 0  - at least one index deleted
 * -1 - indexes for this table on the given attributes haven't been found
 * -2 - internal error
 */
RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -2;
    }
    return destroyIndexes(tableID, std::set<std::string>{attributeName});
}

/*
 * RETURN CODES:
 * 0  - at least one index deleted
 * -1 - indexes for this table on the given attributes haven't been found
 * -2 - internal error
 */
RC RelationManager::destroyIndexes(const unsigned &tableID, const std::set<std::string> &attributeNames) {
    map<string, IndexAttributeInfo> indexAttributes;
    if(processIndexesForTable(tableID, attributeNames, indexAttributes) != 0) {
        return -2;
    }

    bool atLeastOneDeleted = false;
    for(map<string, IndexAttributeInfo>::iterator it = indexAttributes.begin() ; it != indexAttributes.end() ; ++it) {
        if(IndexManager::instance().destroyFile(it->second.filename) != 0) {
            return -2;
        }
        modifySystemTable_AdminRequest = true;
        if(deleteTuple("Indexes", it->second.rid) != 0) {
            return -2;
        }
        modifySystemTable_AdminRequest = false;
        atLeastOneDeleted = true;
    }

    return atLeastOneDeleted ? 0 : -1;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    int tableID = getIdFromTableName(tableName);
    if(tableID < 1) {
        return -1;
    }
    map<string, IndexAttributeInfo> attributes;
    RC rc = processIndexesForTable(tableID, set<string>{attributeName}, attributes);
    if(rc != 0 || attributes.find(attributeName) == attributes.end()) {
        return -1;
    }

    //moze tutaj tez ta moja funkcje process cos tam zeby dostac filename?
    IXFileHandle ixFileHandle;
    rc = IndexManager::instance().openFile(attributes[attributeName].filename, ixFileHandle);
    if(rc != 0) {
        return -1;
    }

    std::vector<Attribute> attrs;
    rc = getAttributes(tableName, attrs);
    if(rc != 0) {
        return -1;
    }
    Attribute attr;
    for(int i = 0 ; i < attrs.size() ; ++i) {
        if(attrs[i].name == attributeName) {
            attr = attrs[i];
        }
    }

    return IndexManager::instance().scan(ixFileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.getIxScanIterator());
}