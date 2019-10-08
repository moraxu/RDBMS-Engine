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
    std::vector<byte> recordFormat;
    transformDataToRecordFormat(recordDescriptor, data, recordFormat);

    unsigned pageNumber;
    unsigned targetSlotNumber;
    byte page[PAGE_SIZE] ;
    memset(page, 0, PAGE_SIZE);
    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2) = 1;


    RC rcode = readFirstFreePage(fileHandle, pageNumber, recordFormat.size(), page, targetSlotNumber);
    if(rcode != 0) {
        return rcode;
    }

    rcode = insertRecordOnPage(fileHandle, recordFormat, recordDescriptor.size(), pageNumber, targetSlotNumber, page);
    if(rcode != 0) {
        return rcode;
    }

    rid.pageNum = pageNumber;
    rid.slotNum = targetSlotNumber;

    return 0;
}

void RecordBasedFileManager::transformDataToRecordFormat(const std::vector<Attribute> &recordDescriptor, const void *data, std::vector<byte> &recordFormat) {
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(recordDescriptor.size()/8.0));
    const byte* actualData = reinterpret_cast<const byte*>(data) + nullInfoFieldLength;
    unsigned actualDataSizeInBytes = 0;

    std::vector<unsigned> fieldOffsets(recordDescriptor.size()+1); //array of field offsets equals to the number of fields + 1 additional offset to the end of the record

    for(unsigned i = 0 ; i < recordDescriptor.size() ; ++i) {
        const byte* byteInNullInfoField = reinterpret_cast<const byte*>(data) + i/8;

        bool nullField = *byteInNullInfoField & (1 << 7-i%8);
        if(nullField) {
            fieldOffsets[i+1] = fieldOffsets[i];

        }
        else {
            if(recordDescriptor[i].type == AttrType::TypeInt || recordDescriptor[i].type == AttrType::TypeReal) {
                fieldOffsets[i+1] = fieldOffsets[i] + recordDescriptor[i].length;
                actualDataSizeInBytes += recordDescriptor[i].length;
            }
            else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned*>(actualData + actualDataSizeInBytes);
                fieldOffsets[i+1] = fieldOffsets[i] + (4 + varCharLength);
                actualDataSizeInBytes += (4 + varCharLength);
            }
        }
    }

    recordFormat.insert(recordFormat.end(),
                        reinterpret_cast<const byte*>(fieldOffsets.data()),
                        reinterpret_cast<const byte*>(fieldOffsets.data()) + fieldOffsets.size()*sizeof(unsigned));

    recordFormat.insert(recordFormat.end(),
                        actualData,
                        actualData + actualDataSizeInBytes);

}

RC RecordBasedFileManager::readFirstFreePage(FileHandle &fileHandle, unsigned &pageNumber, const unsigned recordLength, byte *page, unsigned &targetSlotNumber) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();

    if(numberOfPages > 0) {
        bool lastPageNotAnalyzed = true;

        for(unsigned i = numberOfPages-1 ; i < numberOfPages-1 || lastPageNotAnalyzed ; ) {
            RC rcode = fileHandle.readPage(i, page);
            if(rcode != 0) {
                return rcode;
            }

            unsigned freeSpaceOffset = *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned));
            unsigned slotDirectorySize = *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2);
            unsigned slotDirectoryBeginningOffset = PAGE_SIZE - sizeof(unsigned)*4;

            //we are searching for empty slot so as to determine whether we'd need to create a new slot or not
            bool emptySlotFound = false;
            for(int i = 0, *slot = reinterpret_cast<int*>(page + slotDirectoryBeginningOffset) ; i < slotDirectorySize ; ++i, slot -= 2) {
                if(*slot == -1) {
                    targetSlotNumber = i;
                    emptySlotFound = true;
                    break;
                }
            }

            long totalFreeSpaceInBytes = PAGE_SIZE - sizeof(unsigned)*2 - slotDirectorySize*sizeof(unsigned)*2 - freeSpaceOffset;
            if(!emptySlotFound) {
                totalFreeSpaceInBytes -= sizeof(unsigned)*2;
            }

            if(totalFreeSpaceInBytes >0 && recordLength <= totalFreeSpaceInBytes) {

                pageNumber = i;
                if(!emptySlotFound) {
                    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2) += 1;
                    targetSlotNumber = slotDirectorySize;
                }
                return 0;
            }

            if(i == numberOfPages-1) {
                lastPageNotAnalyzed = false;
                i = 0;
            }
            else {
                ++i;
            }
        }
    }

    memset(page, 0, PAGE_SIZE);
    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2) = 1;
    RC rcode = fileHandle.appendPage(page);
    if(rcode != 0) {
        return rcode;
    }

    pageNumber = fileHandle.getNumberOfPages()-1;
    targetSlotNumber = 0;
    return 0;
}

RC RecordBasedFileManager::insertRecordOnPage(FileHandle &fileHandle, const std::vector<byte> &recordFormat, const unsigned fieldsNo, const unsigned pageNumber, const unsigned targetSlotNumber, byte *page) {
    unsigned freeSpaceOffset = *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned));

    memcpy(page+freeSpaceOffset, recordFormat.data(), recordFormat.size());

    for(unsigned i = 0, *fieldOffset = reinterpret_cast<unsigned*>(page+freeSpaceOffset) ; i < fieldsNo+1 ; ++i, ++fieldOffset) {
        *fieldOffset += (freeSpaceOffset + ((fieldsNo+1) * sizeof(unsigned)));
    }

    *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - targetSlotNumber*sizeof(unsigned)*2) = freeSpaceOffset;
    *reinterpret_cast<unsigned *>(page + PAGE_SIZE - sizeof(unsigned)*3 - targetSlotNumber*sizeof(unsigned)*2) = recordFormat.size();
    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)) += recordFormat.size();

    RC rcode = fileHandle.writePage(pageNumber, page);
    return rcode;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    byte page[PAGE_SIZE];
    RC rcode = fileHandle.readPage(rid.pageNum, page);
    if(rcode != 0) {
        return rcode;
    }

    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(recordDescriptor.size()/8.0));
    std::vector<byte> readData(nullInfoFieldLength, 0);
    unsigned fieldOffsetsLocation = *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - rid.slotNum*sizeof(unsigned)*2);

    for(unsigned i = 0, *fieldOffsets = reinterpret_cast<unsigned*>(page+fieldOffsetsLocation) ; i < recordDescriptor.size() ; ++i, ++fieldOffsets) {
        if(*fieldOffsets == *(fieldOffsets+1)) {
            unsigned byteInNullInfoField = i/8;
            readData[byteInNullInfoField] |= (1 << 7-i%8);
        }
        else {
            if(recordDescriptor[i].type == AttrType::TypeInt || recordDescriptor[i].type == AttrType::TypeReal) {
                readData.insert(readData.end(), page + *fieldOffsets, page + *fieldOffsets + recordDescriptor[i].length);
            }
            else { //recordDescriptor[i].type == AttrType::TypeVarChar
                readData.insert(readData.end(), page + *fieldOffsets, page + *fieldOffsets + 4);
                readData.insert(readData.end(), page + *fieldOffsets + 4, page + *fieldOffsets + 4 + *(page + *fieldOffsets));
            }
        }
    }

    memcpy(data, readData.data(), readData.size());

    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(recordDescriptor.size()/8.0));
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
                actualData += recordDescriptor[i].length;
            }
            else if(recordDescriptor[i].type == AttrType::TypeReal) {
                cout << *reinterpret_cast<const float*>(actualData) << "\t";
                actualData += recordDescriptor[i].length;
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
        cout<<"\n";
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