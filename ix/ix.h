#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

struct indexEntry
{
    string key;
    int ival;
    float fval;
    RID rid;
    unsigned pageNum;
    bool valid;

    indexEntry(){valid = false;}
    indexEntry(const dataEntry &d){
        key = d.key;
        ival = d.ival;
        fval = d.fval;
        rid = d.rid;
        pageNum = -1;
        valid = true;
    }
};

struct dataEntry
{
    string key;
    int ival;
    float fval;
    RID rid;
}

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

private:
    RC transformKeyRIDPair(const Attribute &attribute,dataEntry &de,const void *key,const RID rid,unsigned &keyLen);
    
    RC resolveNewChildEntry(char *bin,indexEntry &newChildEntry,const Attribute attribute,unsigned &iLen);
    
    RC getNewChildEntry(char *bin,const indexEntry newChildEntry,const Attribute attribute,unsigned &iLen);
    
    RC resolveCompositeKey(char *compositeKey,const Attribute &attribute,dataEntry &de,unsigned &cLen);
    
    RC getCompositeKey(char *compositeKey,const Attribute attribute,const dataEntry &de,unsigned &cLen);

    RC createNewRoot(IXFileHandle &ixFileHandle,const indexEntry &newChildEntry,
        const unsigned leftChild,unsigned newRootPageNum);
    
    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
        const dataEntry &target,char *page,unsigned &offset);
    
    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
        const indexEntry &target,char *page,unsigned &offset);
    
    RC splitIndexEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,
        unsigned insertOffset,char *index,char *bin,unsigned iLen,const unsigned pageNumber);
    
    RC splitDataEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,
        const unsigned insertOffset,char *leaf,const char *composite,const unsigned ckLen,const unsigned pageNumber);
    
    RC backtraceInsert(IXFileHandle &ixFileHandle,const unsigned pageNumber,const Attribute &attribute,
        const void *key,const RID &rid,indexEntry &newChildEntry);

class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();
};

class IXFileHandle:public FileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    RC readRootPointer(unsigned &root);
    RC writeRootPointer(const unsigned root);

};

#endif
