#include <iostream>
#include <cstring>
#include <cmath>
#include "rbfm.h"

using namespace std;

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance().closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(recordDescriptor.size()/8));
    const byte* actualData = reinterpret_cast<const byte*>(data) + nullInfoFieldLength;

    for(unsigned i = 0 ; i < recordDescriptor.size() ; ++i) {
        cout << recordDescriptor[i].name << ": ";
        const byte* byteInNullInfoField = reinterpret_cast<const byte*>(data) + i/8;
        bool nullField = *byteInNullInfoField & (1 << 7-i%8);
        if(nullField) {
            cout << "NULL\t";
        }
        else {
            if(recordDescriptor[i].type == AttrType::TypeInt) {
                cout << *reinterpret_cast<const int*>(actualData) << "\t";
                actualData += 4;
            }
            else if(recordDescriptor[i].type == AttrType::TypeReal) {
                cout << *reinterpret_cast<const float*>(actualData) << "\t";
                actualData += 4;
            }
            else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned*>(actualData);
                actualData += 4;
                unsigned char* varCharContent = new unsigned char[varCharLength+1];
                varCharContent[varCharLength] = '\0';
                memcpy(varCharContent, actualData, varCharLength);
                cout << *varCharContent << "\t";
                delete varCharContent;
                actualData += varCharLength;
            }
        }
    }

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}



