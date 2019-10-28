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
    //memset(page, 0, PAGE_SIZE);
    //*reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2) = 1; //size of slot directory

    RC rcode = readFirstFreePage(fileHandle, fileHandle.getNumberOfPages()-1, pageNumber, recordFormat.size(), page, targetSlotNumber);
    if(rcode != 0) {
        return rcode;
    }

    rcode = insertRecordOnPage(fileHandle, recordFormat, pageNumber, targetSlotNumber, page);
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


RC RecordBasedFileManager::readFirstFreePage(FileHandle &fileHandle, unsigned startPage, unsigned &pageNumber, const unsigned recordLength, byte *page, unsigned &targetSlotNumber) {
    unsigned numberOfPages = fileHandle.getNumberOfPages();

    if(numberOfPages > 0) {
        bool lastPageNotAnalyzed = true;

        for(unsigned i = startPage; i < startPage || lastPageNotAnalyzed; ) {
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
                if(*slot == -1) { //we probably don't have to check for slot_length != -1 in here, as there is no case when *slot == -1 && *slot_length == -1
                    targetSlotNumber = i;
                    emptySlotFound = true;
                    break;
                }
            }

            long totalFreeSpaceInBytes = PAGE_SIZE - sizeof(unsigned)*2 - slotDirectorySize*sizeof(unsigned)*2 - freeSpaceOffset;
            if(!emptySlotFound) {
                totalFreeSpaceInBytes -= sizeof(unsigned)*2;
                /* According to C++ standard, integer overflow in ints is undefined,
                 * so it'd be better to replace it with:
                 *  //for subtraction
                 *   #include <limits.h>
                 *   int a = <something>;
                 *   int x = <something>;
                 *   if ((x > 0) && (a < INT_MIN + x)) // `a - x` would underflow
                */
            }

            if(totalFreeSpaceInBytes > 0 && recordLength <= totalFreeSpaceInBytes) {
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
    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)*2) = 1; //size of slot directory
    RC rcode = fileHandle.appendPage(page);
    if(rcode != 0) {
        return rcode;
    }

    pageNumber = fileHandle.getNumberOfPages()-1;
    targetSlotNumber = 0;
    return 0;
}

RC RecordBasedFileManager::insertRecordOnPage(FileHandle &fileHandle, const std::vector<byte> &recordFormat, const unsigned pageNumber, const unsigned targetSlotNumber, byte *page) {
    unsigned freeSpaceOffset = *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned));
    memcpy(page+freeSpaceOffset, recordFormat.data(), recordFormat.size());

    *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - targetSlotNumber*sizeof(unsigned)*2) = freeSpaceOffset;
    *reinterpret_cast<unsigned *>(page + PAGE_SIZE - sizeof(unsigned)*3 - targetSlotNumber*sizeof(unsigned)*2) = recordFormat.size();
    *reinterpret_cast<unsigned*>(page + PAGE_SIZE - sizeof(unsigned)) += recordFormat.size();

    return fileHandle.writePage(pageNumber, page);
}

RC RecordBasedFileManager::shiftRecord(byte *page,const unsigned dataSize, const unsigned slotNumber){
    unsigned *freeSpace = (unsigned *)(page+PAGE_SIZE-sizeof(unsigned));
    unsigned *slotSize = (unsigned *)(page+PAGE_SIZE-2*sizeof(unsigned));
    unsigned *recordOffset = (unsigned *)(page+PAGE_SIZE-2*(slotNumber+2)*sizeof(unsigned));
    unsigned *recordLen = (unsigned *)(page+PAGE_SIZE-(2*slotNumber+3)*sizeof(unsigned));
    unsigned bytesToBeShifted = *freeSpace-(*recordOffset+*recordLen);

    //for overlapping memory blocks, memmove is a safer approach than memcpy,
    //because copying takes place as if an intermediate buffer were used
    memmove(page+*recordOffset+dataSize,page+*recordOffset+*recordLen,bytesToBeShifted);

    //Change slot offsets and field offsets
    int diff = (int)*recordLen-(int)dataSize;
    for(int i = 0;i < *slotSize;i++){
        int *slotOffset = (int *)(page+PAGE_SIZE-2*(i+2)*sizeof(int));
        if(*slotOffset != -1 && *slotOffset > *recordOffset){
            *slotOffset -= diff;
        }
    }
    if(dataSize == 0)
        *recordOffset = -1;
    *freeSpace -= diff;
    *recordLen = dataSize;

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    byte page[PAGE_SIZE];
    RC rcode = fileHandle.readPage(rid.pageNum, page);
    if(rcode != 0) {
        return rcode;
    }
    //int cnt = 1;
    //cout<<cnt++<<endl;

    int fieldOffsetsLocation = *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - rid.slotNum*sizeof(unsigned)*2);
    //Record has already been deleted, so no record to be read!
    if(fieldOffsetsLocation == -1)
        return -1;
    //cout<<cnt++<<endl;

    int recordLen = *(int *)(page + PAGE_SIZE - sizeof(unsigned)*3 - rid.slotNum*sizeof(unsigned)*2);
    //Recursively visit tombstones until a page with real record is found
    if(recordLen == -1){
        RID cur;
        cur.pageNum = *(unsigned *)(page + fieldOffsetsLocation);
        cur.slotNum = *(unsigned *)(page + fieldOffsetsLocation + sizeof(unsigned));
        //cout<<"Current rid: "<<cur.pageNum<<" "<<cur.slotNum<<" "<<cnt++<<endl;
        return readRecord(fileHandle,recordDescriptor,cur,data);
    }

    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(recordDescriptor.size()/8.0));
    std::vector<byte> readData(nullInfoFieldLength, 0);

    for(unsigned i = 0, *fieldOffsets = reinterpret_cast<unsigned*>(page+fieldOffsetsLocation) ; i < recordDescriptor.size() ; ++i, fieldOffsets++) {
        if(*fieldOffsets == *(fieldOffsets+1)) {
            unsigned byteInNullInfoField = i/8;
            readData[byteInNullInfoField] |= (1 << 7-i%8);
        }
        else {
            if(recordDescriptor[i].type == AttrType::TypeInt || recordDescriptor[i].type == AttrType::TypeReal) {
                //cout<<"INT OR REAL: "<<cnt++<<endl;
                byte* beginningOfRecord = page + fieldOffsetsLocation + (recordDescriptor.size()+1)*sizeof(unsigned) + *fieldOffsets;
                readData.insert(readData.end(), beginningOfRecord, beginningOfRecord + recordDescriptor[i].length);
                //cout<<"Value: "<<*(unsigned *)beginningOfRecord<<endl;
            }
            else { //recordDescriptor[i].type == AttrType::TypeVarChar
                //cout<<"STRING: "<<cnt++<<endl;
                byte* beginningOfStrLength = page + fieldOffsetsLocation + (recordDescriptor.size()+1)*sizeof(unsigned) + *fieldOffsets;
                byte* beginningOfStrContent = beginningOfStrLength + sizeof(unsigned);
                readData.insert(readData.end(), beginningOfStrLength, beginningOfStrContent);
                readData.insert(readData.end(), beginningOfStrContent, beginningOfStrContent + *reinterpret_cast<unsigned*>(beginningOfStrLength));
                //cout<<"Value: "<<*(unsigned *)beginningOfStrLength<<" "<<*(char *)beginningOfStrContent<<endl;
            }
        }
    }
    memcpy(data, readData.data(), readData.size());
    return 0;
}

/**
Important Note: The first field(offset field) in slot is set to -1 if deleted;
The second field(length field) is set to -1 if the slot is tombstone. If so, the record content is filled with the actual RID.
**/
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
    onst RID &rid) {
    unsigned p = rid.pageNum,s = rid.slotNum;
    byte pageStart[PAGE_SIZE];
    RC rcode = fileHandle.readPage(p,pageStart);
    if(rcode != 0) {
        return rcode;
    }

    //NOTE: record offset and record length can be -1 so that they have to be treated as integers
    int *recordOffset = (int *)(pageStart+PAGE_SIZE-2*(s+2)*sizeof(unsigned));
    int *recordLen = (int *)(pageStart+PAGE_SIZE-(2*s+3)*sizeof(unsigned));

    if(*recordLen == -1){
        RID cur;
        cur.pageNum = *(unsigned *)(pageStart+*recordOffset);
        cur.slotNum = *(unsigned *)(pageStart+*recordOffset+sizeof(unsigned));
        //If chain of deleteRecord calls completes successfully, we have to unmark directory slot
        //that was pointing to a tombstone, otherwise, if someone called the deleteRecord for the second time on
        //the same RID, it theoretically could mess up things in other files
        if(deleteRecord(fileHandle,recordDescriptor,cur) != -1) {
            *recordLen = 0;
            //If previous chain of calls to delete a record succeeded, we can delete tombstones, by shifting
            //the remaining records 8 bytes towards the beginning of the page.
            //However, in a perfect program, the deletion of tombstones (and thus of the record) would have to be reverted,
            //if we spotted an error code in the process. It complicates the program significantly, though, and in our case
            //such error test cases will not take place anyway.
            shiftRecord(pageStart, 0, s);
            return fileHandle.writePage(p,pageStart);
        } else {
            return -1;
        }
    }
        //If the record has not been deleted,then delete it
    else if(*recordOffset != -1){
        //Shift records
        shiftRecord(pageStart,0,s);
        return fileHandle.writePage(p,pageStart);
    }
    else {
        return -1; //trying to delete a deleted record
    }
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
                cout << *reinterpret_cast<const int*>(actualData) << '\t';
                actualData += recordDescriptor[i].length;
            }
            else if(recordDescriptor[i].type == AttrType::TypeReal) {
                cout << *reinterpret_cast<const float*>(actualData) << '\t';
                actualData += recordDescriptor[i].length;
            }
            else { //recordDescriptor[i].type == AttrType::TypeVarChar
                unsigned varCharLength = *reinterpret_cast<const unsigned*>(actualData);
                actualData += 4;
                string varCharContent = string((const char *)actualData,varCharLength);
                cout<<varCharContent<<'\t';
                actualData += varCharLength;
            }
        }
    }
    cout<<"\n";

    return 0;
}

RC RecordBasedFileManager::moveToNextAvailableRecord(FileHandle& fileHandle, RID& rid) {
    //We first consider the position given in rid itself
    for( ; rid.pageNum < fileHandle.getNumberOfPages() ; ++rid.pageNum, rid.slotNum=0) {
        byte page[PAGE_SIZE];
        RC rcode = fileHandle.readPage(rid.pageNum,page);
        if(rcode != 0) {
            return -1;
        }
        unsigned slotDirectorySize = *reinterpret_cast<unsigned *>(page + PAGE_SIZE - sizeof(unsigned)*2);
        while(rid.slotNum < slotDirectorySize && *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - rid.slotNum*sizeof(unsigned)*2) == -1) {
            ++rid.slotNum;
        }
        if(rid.slotNum < slotDirectorySize) {
            return 0; //we found non-empty slot
        }
    }
    return RBFM_EOF;
}

/**
Important Note: The first field(offset field) in slot is set to -1 if deleted;
The second field(length field) is set to -1 if the slot is tombstone. If so, the record content is filled with the actual RID.
**/
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {

    vector<byte> formattedData;
    transformDataToRecordFormat(recordDescriptor,data,formattedData);
    unsigned dataSize =  formattedData.size();
    //int cnt = 1;
    //cout<<cnt++<<endl;

    unsigned p = rid.pageNum,s = rid.slotNum;
    byte pageStart[PAGE_SIZE];
    RC rc = fileHandle.readPage(p,pageStart);
    if(rc != 0)
        return rc;

    unsigned *freeSpace = (unsigned *)(pageStart+PAGE_SIZE-sizeof(unsigned));
    unsigned *slotSize = (unsigned *)(pageStart+PAGE_SIZE-2*sizeof(unsigned));
    int *recordOffset = (int *)(pageStart+PAGE_SIZE-2*(s+2)*sizeof(int));
    int *recordLen = (int *)(pageStart+PAGE_SIZE-(2*s+3)*sizeof(int));

    //If the record rid refers to doesn't exist,return
    if(*recordOffset == -1) return -1;
    //cout<<cnt++<<endl;
    //If the slot is a tombstone,loop until it's not a tombstone
    if(*recordLen == -1){
        RID cur;
        cur.pageNum = *(unsigned *)(pageStart+*recordOffset);
        cur.slotNum = *(unsigned *)(pageStart+*recordOffset+sizeof(unsigned));
        //cout<<cnt++<<endl;
        return updateRecord(fileHandle,recordDescriptor,data,cur);
    }

    if(*recordLen >= dataSize){
        //cout<<"Left shift: "<<cnt++<<endl;
        memcpy(pageStart+*recordOffset,formattedData.data(),dataSize);
        //Shift towards the begining of page
        if(*recordLen > dataSize) {
            return shiftRecord(pageStart,dataSize,s);
        }
        return 0;
    }
    else {
        //Check if there is enough free space in this page for the augmentation
        //If so, do not use tombstone
        unsigned freeSpaceLeft = PAGE_SIZE-2*sizeof(unsigned)-2*sizeof(unsigned)*(*slotSize)-*freeSpace;

        if(freeSpaceLeft >= dataSize-*recordLen){
            //Shift towards the end of page
            //cout<<"freeSpace enough: "<<cnt++<<endl;
            shiftRecord(pageStart,dataSize,s);
            memcpy(pageStart+*recordOffset,formattedData.data(),dataSize);
        }else{
            //Find free space for the update in another page, also use tombstone
            //cout<<"freeSpace not enough: "<<cnt++<<endl;
            unsigned pageNumber,slotNumber;
            byte page[PAGE_SIZE];
            unsigned upper = fileHandle.getNumberOfPages();

            rc = readFirstFreePage(fileHandle,p+1 == upper?0:p+1,pageNumber,dataSize,page,slotNumber);
            if(rc != 0)
                return rc;

            rc = insertRecordOnPage(fileHandle,formattedData,pageNumber,slotNumber,page);
            if(rc != 0)
                return rc;

            //delete original record with the RID that points to the moved record
            //Note: the length of the original record must be greater than 2*sizeof(unsigned)
            shiftRecord(pageStart,2*sizeof(unsigned),s);
            *(unsigned *)(pageStart+*recordOffset) = pageNumber;
            *(unsigned *)(pageStart+*recordOffset+sizeof(unsigned)) = slotNumber;

            //set length field to -1 as a indicator that this slot is a tombstone
            *recordLen = -1;
        }

        return fileHandle.writePage(p,pageStart);
    }
}

/**
Important Note: This is readRecord lookalike, it extracts only the column names with the indices listed in "attributesToExtract" vector.
**/
RC RecordBasedFileManager::filterAttributes(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data, const std::vector<unsigned> &attributesToExtract) {
    if(attributesToExtract.empty()) { //just as precaution
        return 0;
    }

    byte page[PAGE_SIZE];
    RC rcode = fileHandle.readPage(rid.pageNum, page);
    if(rcode != 0) {
        return rcode;
    }

    int fieldOffsetsLocation = *reinterpret_cast<int*>(page + PAGE_SIZE - sizeof(unsigned)*4 - rid.slotNum*sizeof(unsigned)*2);
    if(fieldOffsetsLocation == -1)
        return -1;

    int recordLen = *(int *)(page + PAGE_SIZE - sizeof(unsigned)*3 - rid.slotNum*sizeof(unsigned)*2);
    if(recordLen == -1){
        RID cur;
        cur.pageNum = *(unsigned *)(page + fieldOffsetsLocation);
        cur.slotNum = *(unsigned *)(page + fieldOffsetsLocation + sizeof(unsigned));
        return filterAttributes(fileHandle,recordDescriptor,cur,data, attributesToExtract);
    }

    const unsigned nullInfoFieldLength = static_cast<unsigned>(ceil(attributesToExtract.size()/8.0));
    std::vector<byte> readData(nullInfoFieldLength, 0);

    for(unsigned i = 0, *fieldOffsets = reinterpret_cast<unsigned*>(page+fieldOffsetsLocation) ; i < attributesToExtract.size() ; ++i, fieldOffsets = reinterpret_cast<unsigned*>(page+fieldOffsetsLocation)) {
        unsigned attrInd = attributesToExtract[i];
        if(i == 0) fieldOffsets += attributesToExtract[i];
        else fieldOffsets += (attributesToExtract[i]-attributesToExtract[i-1]);

        if(*fieldOffsets == *(fieldOffsets+1)) {
            unsigned byteInNullInfoField = i/8;
            readData[byteInNullInfoField] |= (1 << 7-i%8);
        }
        else {
            if(recordDescriptor[attrInd].type == AttrType::TypeInt || recordDescriptor[attrInd].type == AttrType::TypeReal) {
                byte* beginningOfRecord = page + fieldOffsetsLocation + (recordDescriptor.size()+1)*sizeof(unsigned) + *fieldOffsets;
                readData.insert(readData.end(), beginningOfRecord, beginningOfRecord + recordDescriptor[attrInd].length);
            }
            else { //recordDescriptor[attrInd].type == AttrType::TypeVarChar
                byte* beginningOfStrLength = page + fieldOffsetsLocation + (recordDescriptor.size()+1)*sizeof(unsigned) + *fieldOffsets;
                byte* beginningOfStrContent = beginningOfStrLength + sizeof(unsigned);
                readData.insert(readData.end(), beginningOfStrLength, beginningOfStrContent);
                readData.insert(readData.end(), beginningOfStrContent, beginningOfStrContent + *reinterpret_cast<unsigned*>(beginningOfStrLength));
            }
        }
    }
    memcpy(data, readData.data(), readData.size());
    return 0;
}

//The actual data in *data is also prefixed with a null byte, just as in format used in the other RecordBasedFileManager's methods
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
    const RID &rid, const std::string &attributeName, void *data) {
    unsigned attributeIndex = -1;
    for(int i = 0 ; i < recordDescriptor.size() ; ++i) {
        if(recordDescriptor[i].name == attributeName) {
            attributeIndex = i;
            break;
        }
    }
    if(attributeIndex == -1) {
        return -1;
    }
    return filterAttributes(fileHandle, recordDescriptor, rid, data, std::vector<unsigned>(1, attributeIndex));
}

//This is a bad design, but we have to follow that. We basically need to use scan to initialize/change RBFM_ScanIterator,
//I only introduced setters/getters in order not to declare friendship between classes. We're not supposed to invoke
//RBFM_ScanIterator's setter classes on their own, because at the end of our changes we should also invoke RBFM_ScanIterator::fillAttrIndices(), like here
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const std::vector<std::string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.setFileHandle(fileHandle);
    rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);
    rbfm_ScanIterator.setConditionAttribute(conditionAttribute);
    rbfm_ScanIterator.setCompOp(compOp);
    rbfm_ScanIterator.setValue(value);
    rbfm_ScanIterator.setAttributeNames(attributeNames);
    rbfm_ScanIterator.fillAttrIndices();
    return 0;
}

//We do some preprocessing of the projected attributes and the attribute used for comparison
//- we save their default IDs in recordDescriptor vector in order to use
//them later: in RBFM_ScanIterator::getNextRecord and RecordBasedFileManager::filterAttributes
void RBFM_ScanIterator::fillAttrIndices() {
    for(int i = 0 ; i < recordDescriptor.size() ; ++i) {
        if(attributeNames.find(recordDescriptor[i].name) != attributeNames.end()) {
            attrToExtractInd.push_back(i);
        }
        if(recordDescriptor[i].name == conditionAttribute) {
            attrForCompInd = i;
        }
    }
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    byte record[PAGE_SIZE]; //we have to read the attribute for comparison into an array of a page size
                            //because in case of a string attribute, it would be hard to predict its size

    //Throughout the function, we keep iterating over RIDs and keep the most current one in currRID.
    //At the end of the function, we copy its members (page number and slot number) into "rid" parameter.
    //Then we increment slot number of the currID (++currRID.slotNum) so that next invocation of this function
    //read next record in the file (if there is any)

    for( ; true ; ++currRID.slotNum) { //iterate over table till we find next record satisfying the condition
        if(RecordBasedFileManager::instance().moveToNextAvailableRecord(fileHandle, currRID) == RBFM_EOF) {
            return RBFM_EOF;
        }
        RC rc = RecordBasedFileManager::instance().readAttribute(fileHandle, recordDescriptor, currRID, conditionAttribute, record);
        if(rc != 0) {
            return rc;
        }
        if(record[0] != 0) { //some bit is set in the null info field
            continue;
        }

        if(recordDescriptor[attrForCompInd].type == AttrType::TypeInt) {
            if(performCompOp(*reinterpret_cast<const int*>(value), *reinterpret_cast<int*>(record+1))) {
                break;
            }
        }
        else if(recordDescriptor[attrForCompInd].type == AttrType::TypeReal) {
            if(performCompOp(*reinterpret_cast<const float*>(value), *reinterpret_cast<float*>(record+1))) {
                break;
            }
        }
        else { //recordDescriptor[attrForCompInd].type == AttrType::TypeVarChar
            unsigned* recordLen = reinterpret_cast<unsigned *>(record+1);
            char* recordCont = reinterpret_cast<char*>(record+1+sizeof(unsigned));
            const unsigned* valueLen = reinterpret_cast<const unsigned*>(value);
            const char* valueCont = reinterpret_cast<const char*>(value+sizeof(unsigned));
            if(performCompOp(string(valueCont, *valueLen), string(recordCont, *recordLen))) {
                break;
            }
        }
    }
    RC rc = RecordBasedFileManager::instance().filterAttributes(fileHandle, recordDescriptor, currRID, data, attrToExtractInd);
    if(rc != 0) {
        return rc;
    }
    rid.slotNum = currRID.slotNum;
    rid.pageNum = currRID.pageNum;
    ++currRID.slotNum;
    return 0;
}

/*
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;
 */
template <typename T>
bool RBFM_ScanIterator::performCompOp(const T& value, const T& actualValue) {
    if(compOp == CompOp::EQ_OP) {
        return actualValue == value;
    }
    else if(compOp == CompOp::LT_OP) {
        return actualValue < value;
    }
    else if(compOp == CompOp::LE_OP) {
        return actualValue <= value;
    }
    else if(compOp == CompOp::GT_OP) {
        return actualValue > value;
    }
    else if(compOp == CompOp::GE_OP) {
        return actualValue >= value;
    }
    else if(compOp == CompOp::NE_OP) {
        return actualValue != value;
    }
    return true; //(compOp == CompOp::NO_OP)
}