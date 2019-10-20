#include <iostream>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <cstdio>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

void *record = malloc(2000);
void *returnedData = malloc(2000);
std::vector<Attribute> recordDescriptor;
unsigned char *nullsIndicator = NULL;
FileHandle fileHandle;

void readRecord(RecordBasedFileManager &rbfm, const RID &rid, const std::string &str) {
    int recordSize;
    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
                  record, &recordSize);
    rbfm.printRecord(recordDescriptor,record);

    RC rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    rbfm.printRecord(recordDescriptor,returnedData);

    assert(rc == success && "Reading a record should not fail.");

    // Compare whether the two memory blocks are the same
    assert(memcmp(record, returnedData, recordSize) == 0 && "Returned Data should be the same");
}

void insertRecord(RecordBasedFileManager &rbfm, RID &rid, const std::string &str) {
    int recordSize;
    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
                  record, &recordSize);

    RC rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

}

void updateRecord(RecordBasedFileManager &rbfm, RID &rid, const std::string& str) {
    int recordSize;
    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
                  record, &recordSize);

    RC rc = rbfm.updateRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Updating a record should not fail.");

}

int RBFTest_Update(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    std::cout << std::endl << "***** In RBF Test Case Update *****" << std::endl;

    RC rc;
    std::string fileName = "test_update";

    // Create a file
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    createRecordDescriptor(recordDescriptor);
    recordDescriptor[0].length = (AttrLength) 1000;

    std::string longStr;
    for (int i = 0; i < 1000; i++) {
        longStr.push_back('a');
    }

    std::string shortStr;
    for (int i = 0; i < 10; i++) {
        shortStr.push_back('s');
    }

    std::string midString;
    for (int i = 0; i < 100; i++) {
        midString.push_back('m');
    }

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert short record
    insertRecord(rbfm, rid, shortStr);
    RID shortRID = rid;
    cout<<"shortRID: "<<shortRID.pageNum<<" "<<shortRID.slotNum<<endl;

    // Insert mid record
    insertRecord(rbfm, rid, midString);
    RID midRID = rid;
    cout<<"midRID: "<<midRID.pageNum<<" "<<shortRID.slotNum<<endl;

    // Insert long record
    insertRecord(rbfm, rid, longStr);

    // update short record
    updateRecord(rbfm, shortRID, midString);
    cout<<"Update1 no segmentation fault!"<<endl;

    //read updated short record and verify its content
    readRecord(rbfm, shortRID, midString);
    cout<<"Read1 no segmentation fault!"<<endl;

    // insert two more records
    insertRecord(rbfm, rid, longStr);
    insertRecord(rbfm, rid, longStr);

    // read mid record and verify its content
    readRecord(rbfm, midRID, midString);
    cout<<"Read2 no segmentation fault!"<<endl;

    // update short record
    updateRecord(rbfm, shortRID, longStr);
    cout<<"Update2 no segmentation fault!"<<endl;

    // read the short record and verify its content
    readRecord(rbfm, shortRID, longStr);
    cout<<"Read3 no segmentation fault!"<<endl;

    // delete the short record
    rbfm.deleteRecord(fileHandle, recordDescriptor, shortRID);
    cout<<"Delete no segmentation fault!"<<endl;

    // verify the short record has been deleted
    rc = rbfm.readRecord(fileHandle, recordDescriptor, shortRID, returnedData);
    cout<<"Read4 no segmentation fault!"<<endl;

    assert(rc != success && "Read a deleted record should not success.");

    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    free(record);
    free(returnedData);
    free(nullsIndicator);

    std::cout << "RBF Test Case Update Finished! The result will be examined." << std::endl << std::endl;

    return 0;
}

int main() {
    // To test the functionality of the record-based file manager
    remove("test_update");
    return RBFTest_Update(RecordBasedFileManager::instance());
}