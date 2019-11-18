#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdio.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

using namespace std;

struct dataEntry
{
    string key;
    int ival;
    float fval;
    RID rid;

    dataEntry(){
        ival = -1;
        fval = -1;
        rid.pageNum = 0;
        rid.slotNum = 0;
    }

    bool operator!=(const dataEntry& dataEnt) const {
        return !(key == dataEnt.key
                && ival == dataEnt.ival
                && fval == dataEnt.fval
                && rid.slotNum == dataEnt.rid.slotNum
                && rid.pageNum == dataEnt.rid.pageNum);
    }
};

struct indexEntry
{
    string key;
    int ival;
    float fval;
    RID rid;
    unsigned pageNum;
    bool valid;

    indexEntry(){
    	ival = -1;
    	fval = -1;
    	rid.pageNum = 0;
    	rid.slotNum = 0;
    	pageNum = 0;
    	valid = false;
    }

    indexEntry(const dataEntry &d){
        key = d.key;
        ival = d.ival;
        fval = d.fval;
        rid = d.rid;
        pageNum = 0;
        valid = true;
    }

    unsigned actualLength(const Attribute& attribute) const {
        unsigned result = 0;
        if(attribute.type == AttrType::TypeInt) {
            result += sizeof(ival);
        }
        else if(attribute.type == AttrType::TypeReal) {
            result += sizeof(fval);
        }
        else {
            result += (key.length() + sizeof(unsigned));
        }
        result += sizeof(pageNum);
        result += sizeof(rid);
        return result;
    }
};


/**
Hidden page format:
    [ ixReadPageCounter = 0 || ixWritePageCounter = 0 || ixAppendPageCounter = 0
    || number of pages || lastTableID || dummy node pointing to the root of the index ]
This function reads root pointer from hidden page.
**/
class IXFileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    unsigned noPages;
    unsigned lastTableID;
    unsigned rootPage;
    FILE *fp;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    bool isValid() { return fp != NULL; }

    RC readPage(PageNum pageNum, void *data);

    RC writePage(PageNum pageNum, const void *data);

    RC appendPage(const void *data);

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    unsigned getNumberOfPages() {return noPages;}
};

class IX_ScanIterator {
    IXFileHandle* ixFileHandle;
    Attribute attribute;
    //These data entry objects contain the value from lowKey/highKey - so that we don't have to cast every time
    dataEntry lowKeyEntry;
    bool lowKeyInfinity = false;
    dataEntry highKeyEntry;
    bool highKeyInfinity = false;
    bool lowKeyInclusive;
    bool highKeyInclusive;

    /*
     * Scanning indicates whether a scanner is currently in scanning state.
     * If a scanner is already in scanning state,it should not be assigned with another scan task.
     */
    bool scanning;
    bool isNewPage;

    unsigned currPage;
    unsigned currOffset;
    unsigned lastReadFreeSpaceOffset;
    unsigned lastReadDataEntryLength;

    RC determineInitialPageAndOffset();

    // Assigns to pageNo the number of first leaf page or return -1 if there aren't any pages in the tree
    RC findFirstLeafPage(IXFileHandle& fileHandle, unsigned& pageNo);

    void transformDataEntryKey(dataEntry dataEnt, void* key) const;

public:
    const IXFileHandle* getIxFileHandle() const;

    void setIxFileHandle(IXFileHandle* ixFileHandle);

    const Attribute &getAttribute() const;

    void setAttribute(const Attribute attribute);

    const dataEntry &getLowKeyEntry() const;

    void setLowKeyEntry(const dataEntry &lowKeyEntry);

    const dataEntry &getHighKeyEntry() const;

    void setHighKeyEntry(const dataEntry &highKeyEntry);

    bool isLowKeyInclusive() const;

    void setLowKeyInclusive(bool lowKeyInclusive);

    bool isHighKeyInclusive() const;

    void setHighKeyInclusive(bool highKeyInclusive);

    bool isLowKeyInfinity() const;

    void setLowKeyInfinity(bool lowKeyInfinity);

    bool isHighKeyInfinity() const;

    void setHighKeyInfinity(bool highKeyInfinity);

    bool isScanning() const;

    void setScanning(bool scanStarted);

    bool isIsNewPage() const;

    void setIsNewPage(bool newPage);

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();
};

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

    // Assigns to pageNo the number of the leaf page that SHOULD contain the given key
    RC searchIndexTree(IXFileHandle& fileHandle, const unsigned pageNumber,
    		const Attribute& attribute,const bool lowKeyInclusive, const dataEntry& dataEnt, unsigned& leafPageNo);

    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                   const dataEntry &target,char *page,unsigned &offset);

    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
    		const indexEntry &target,const bool lowKeyInclusive,char *page,unsigned &offset);

    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                   const dataEntry &target,const bool lowKeyInclusive,char *page,unsigned &offset);

    RC resolveCompositeKey(char *compositeKey,const Attribute &attribute,dataEntry &de,unsigned &cLen) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

private:
    RC deleteEntryHelper(IXFileHandle &ixFileHandle, const unsigned pageNumber, const Attribute &attribute, const dataEntry& dataEnt, bool amIRoot);

    RC transformKeyRIDPair(const Attribute &attribute,dataEntry &de,const void *key,const RID rid,unsigned &keyLen);
    
    RC resolveNewChildEntry(char *bin,indexEntry &newChildEntry,const Attribute attribute,unsigned &iLen) const;
    
    RC getNewChildEntry(char *bin,const indexEntry newChildEntry,const Attribute attribute,unsigned &iLen);
    
    RC getCompositeKey(char *compositeKey,const Attribute attribute,const dataEntry &de,unsigned &cLen);

    RC createNewRoot(IXFileHandle &ixFileHandle,const indexEntry &newChildEntry,
    		const Attribute attribute,const unsigned leftChild,unsigned &newRootPageNum);
    
    RC searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
    		const indexEntry &target,char *page,unsigned &offset);
    
    RC splitIndexEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,const Attribute attribute,
        unsigned insertOffset,char *index,char *bin,const unsigned iLen,const unsigned pageNumber);
    
    RC splitDataEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,const Attribute attribute,
        const unsigned insertOffset,char *leaf,const char *composite,const unsigned ckLen,const unsigned pageNumber);
    
    RC backtraceInsert(IXFileHandle &ixFileHandle,const unsigned pageNumber,const Attribute &attribute,
        const void *key,const RID &rid,indexEntry &newChildEntry);

    void printTab(const unsigned level) const;

    string RIDtoStr(const RID &rid) const;

    void printNode(IXFileHandle &ixFileHandle, const Attribute &attribute,
    		const unsigned pageNumber,const unsigned level) const;
};

#endif
