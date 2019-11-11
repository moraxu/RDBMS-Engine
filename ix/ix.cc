#include "ix.h"

PagedFileManager pfm = PagedFileManager::instance();

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

/**
Hidden page format:
    [ ixReadPageCounter = 0 || ixWritePageCounter = 0 || ixAppendPageCounter = 0 
    || number of pages || dummy node list pointing to the root of indexes ]
**/
RC IndexManager::createFile(const std::string &fileName) {
    //File already exists!
    if(access(fileName.c_str(),F_OK) == 0)
        return -1;
    
    FILE *fp = fopen(fileName.c_str(),"wb+");
    //File open error!
    if(!fp)
        return -1;

    byte cnt[PAGE_SIZE];
    memset(cnt,0,sizeof(cnt));
    fseek(fp,0,SEEK_SET);
    fwrite(cnt,1,PAGE_SIZE,fp);
    fclose(fp);
    return 0;
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return pfm.destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    return pfm.openFile(fileName,ixFileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return pfm.closeFile(ixFileHandle);
}

/***
IMPORTANT NOTE:
In index file,slot directory is not used since it's not worth using slot directory to speed up the search process!

For non-leaf index page,the page format is:
    [ page number ||  key value || page number || ... ||page number ]
Note that in order to deal with dulplicate issues, our search key should be designed as (key,rid) pairs
In addition,
the last 0-3 bytes in a non-leaf index page are used to indicate the first unused byte,
the last 4-7 bytes are used to indicate whether it's a leaf page(1 for yes and 0 for no);
For leaf index page,we use Alternative 3,and the page format is:
    [ key value || rid list size || rid list ],...
In addition,
the last 0-3 bytes in a non-leaf index page are used to indicate the first unused byte,
the last 4-7 bytes are used to indicate whether it's a leaf page,
the last 8-11 bytes are used to indicate the page number of its right sibling,
and the last 12-15 bytes are used to indicate the page number of its left sibling.
***/

RC IndexManager::transformKeyRIDPair(const Attribute &attribute,dataEntry &de,const void *key,const RID rid,unsigned &keyLen){
    if(attribute.type == AttrType::TypeInt){
        de.ival = *(int *)key;
        keyLen = sizeof(int)+sizeof(RID);
    }
    else if(attribute.type == AttrType::TypeReal){
        de.fval = *(float *)key;
        keyLen = sizeof(float)+sizeof(RID);
    }else{
        unsigned strLen = *(unsigned *)key;
        de.key = string((char *)key,strLen+sizeof(unsigned));
        keyLen = sizeof(unsigned)+strLen+sizeof(RID);
    }
    de.rid = rid;
    return 0;
}

/**
The following five functions transform between binary stream and dataEntry/indexEntry struct
**/
RC resolveNewChildEntry(char *bin,indexEntry &newChildEntry,const Attribute attribute,unsigned &iLen){
    char *cur = bin;
    if(attribute.type == AttrType::TypeInt){
        newChildEntry.ival = *(int *)cur++;
    }
    else if(attribute.type == AttrType::TypeReal){
        newChildEntry.fval = *(float *)cur++;
    }else{
        unsigned strLen = *(unsigned *)cur;
        newChildEntry.key = string(cur,strLen+sizeof(unsigned));
        cur += strLen+sizeof(unsigned);
    }
    newChildEntry.rid.pageNum = *(unsigned *)cur++;
    newChildEntry.rid.slotNum = *(unsigned *)cur++;
    newChildEntry.pageNum = *(unsigned *)cur++;
    newChildEntry.valid = true;
    iLen = cur-bin;
    return 0;   
}

RC getNewChildEntry(char *bin,const indexEntry newChildEntry,const Attribute attribute,unsigned &iLen){
    if(newChildEntry.valid){
        char *cur = bin;
        if(attribute.type == AttrType::TypeInt){
            *(int *)cur++ = newChildEntry.ival;
        }
        else if(attribute.type == AttrType::TypeReal){
            *(float *)cur++ = newChildEntry.fval;
        }else{
            unsigned strLen = newChildEntry.key.length();
            memcpy(cur,newChildEntry.key.c_str(),strLen);
            cur += strLen;
        }
        newChildEntry.rid.pageNum = *(unsigned *)cur++;
        newChildEntry.rid.slotNum = *(unsigned *)cur++;
        newChildEntry.pageNum = *(unsigned *)cur++;
        iLen = cur-bin;
        return 0;
    }else{
        bin = nullptr;
        iLen = 0;
        return -1;
    }
}

RC resolveCompositeKey(char *compositeKey,const Attribute &attribute,dataEntry &de,unsigned &cLen){
    char *composite = compositeKey;
    if(attribute.type == AttrType::TypeInt){
        de.ival = *(int *)composite++;
    }
    else if(attribute.type == AttrType::TypeReal){
        de.fval = *(float *)composite++;
    }else{
        unsigned strLen = *(unsigned *)composite;
        de.key = string(composite,strLen+sizeof(unsigned)); 
        composite += strLen+sizeof(unsigned);
    }
    de.rid.pageNum = *(unsigned *)composite++;
    de.rid.slotNum = *(unsigned *)composite++;
    cLen = composite-compositeKey;

    return 0;
}

RC getCompositeKey(char *compositeKey,const Attribute attribute,const dataEntry &de,unsigned &cLen){
    char *composite = compositeKey;
    if(attribute.type == AttrType::TypeInt){
        *(int *)composite++ = de.ival;
    }
    else if(attribute.type == AttrType::TypeReal){
        *(float *)composite++ = de.fval;
    }else{
        unsigned strLen = *(unsigned *)de.key.c_str();
        memcpy(composite,de.key.c_str(),strLen+sizeof(unsigned));
        composite += strLen+sizeof(unsigned);
    }
    *(unsigned *)composite++ = rid.pageNum;
    *(unsigned *)composite++ = rid.slotNum;
    cLen = composite-compositeKey;

    return 0;
}
/**
End of these five functions.
**/

RC createNewRoot(IXFileHandle &ixFileHandle,const indexEntry &newChildEntry,const unsigned leftChild,unsigned newRootPageNum){
    char root[PAGE_SIZE];
    char *cur = root;

    //copy the left child node pointer into new root node
    *(unsigned *)cur++ = leftChild;

    //copy newChildEntry into new root node
    char bin[PAGE_SIZE];
    unsigned iLen;
    getNewChildEntry(bin,newChildEntry,attribute,iLen);
    memcpy(cur,bin,iLen);
    cur += iLen;

    *(unsigned *)(root+PAGE_SIZE-sizeof(unsigned)) = cur-root;
    *(unsigned *)(root+PAGE_SIZE-2*sizeof(unsigned)) = 0;
    
    int rc = ixFileHandle.appendPage(root);
    if(rc != 0)
        return -1;
    newRootPageNum = ixFileHandle.getNumberOfPages()-1;
    return 0;
}

//Search for the offset where the first data entry >= target entry
RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,const dataEntry &target,char *page,unsigned &offset){
    unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));

    char *cur = page;
    dataEntry de;
    unsigned iLen;
    if(attribute.type == AttrType::TypeInt){
        if(cur-page < freeSpaceOffset)
            resolveCompositeKey(cur,attribute,de,iLen);
        while(cur-page < freeSpaceOffset && de.ival <= target.ival){
            unsigned pno = de.rid.pageNum;
            unsigned slno = de.rid.slotNum;
            //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(de.ival == target.ival && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset)
                resolveCompositeKey(cur,attribute,de,iLen);
        }
    }else if(attribute.type == AttrType::TypeReal){
        if(cur-page < freeSpaceOffset)
            resolveCompositeKey(cur,attribute,de,iLen);
        while(cur-page < freeSpaceOffset && de.fval <= target.fval){
            unsigned pno = de.rid.pageNum;
            unsigned slno = de.rid.slotNum;           //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(de.fval == target.fval && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset)
                resolveCompositeKey(cur,attribute,de,iLen);
        }
    }else{
        if(cur-page < freeSpaceOffset)
            resolveCompositeKey(cur,attribute,de,iLen);
        while(cur-page < freeSpaceOffset && de.key <= target.key){
            unsigned pno = de.rid.pageNum;
            unsigned slno = de.rid.slotNum;
            //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(de.key == target.key && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset)
                resolveCompositeKey(cur,attribute,de,iLen);
        }
    }
    //Eventually the offset where the first data entry >= target entry is found.
    offset = cur-page;

    return 0;
}

//Search for the offset where the first index entry >= target entry
RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,const indexEntry &target,char *page,unsigned &offset){
    unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));

    //Skip the first node pointer
    char *cur = page+sizeof(unsigned);
    indexEntry ie;
    unsigned iLen;
    if(attribute.type == AttrType::TypeInt){
        if(cur-page < freeSpaceOffset)
            resolveNewChildEntry(cur,ie,attribute,iLen);
        while(cur-page < freeSpaceOffset && ie.ival <= target.ival){
            unsigned pno = ie.rid.pageNum;
            unsigned slno = ie.rid.slotNum;
            //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(ie.ival == target.ival && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset)
                resolveNewChildEntry(cur,ie,attribute,iLen);
        }
    }else if(attribute.type == AttrType::TypeReal){
        if(cur-page < freeSpaceOffset)
            resolveNewChildEntry(cur,ie,attribute,iLen);
        while(cur-page < freeSpaceOffset && ie.fval <= target.fval){
            unsigned pno = ie.rid.pageNum;
            unsigned slno = ie.rid.slotNum;           //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(ie.fval == target.fval && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset) 
                resolveNewChildEntry(cur,ie,attribute,iLen);
        }
    }else{
        if(cur-page < freeSpaceOffset)
            resolveNewChildEntry(cur,ie,attribute,iLen);
        while(cur-page < freeSpaceOffset && ie.key <= target.key){
            unsigned pno = ie.rid.pageNum;
            unsigned slno = ie.rid.slotNum;
            //When key equals and provided rid > rid inside (key,rid) pairs we skip out.
            if(ie.key == target.key && (pno < target.rid.pageNum || 
                    (pno == target.rid.pageNum && slno < target.rid.slotNum)))
                break;

            cur += iLen;
            if(cur-page < freeSpaceOffset) 
                resolveNewChildEntry(cur,ie,attribute,iLen);
        }
    }
    //Eventually the place where the first data entry >= target entry is found.
    offset = cur-page;

    return 0;
}

/**
The following two 'split' functions split non-leaf/leaf nodes.To find 'the middle record' within certain nodes,
we scan from the beginning due to the lack of slot directory.
The original node is named N,and the newly created one N2. 'index' stores data within N,and 'newn' stores data within N2.
**/
RC IndexManager::splitIndexEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,
    unsigned insertOffset,char *index,char *bin,unsigned iLen,const unsigned pageNumber){
    //Insert entry into the leaf node
    char data[2*PAGE_SIZE]; //Auxiliary double-size page
    char newn[PAGE_SIZE];
    unsigned freeSpaceOffset = *(unsigned *)(index+PAGE_SIZE-sizeof(unsigned));
    unsigned spaceToBeSplit = freeSpaceOffset+iLen;
    memcpy(data,index,insertOffset);
    memcpy(data+insertOffset,bin,iLen);
    memcpy(data+insertOffset+iLen,index+insertOffset,spaceToBeSplit-insertOffset);
    
    //Search for 'the middle point'
    //The search process can be omptimized by starting from the insert place
    char *cur = data;
    unsigned spaceLeft = 0;
    while(spaceLeft < spaceToBeSplit/2.0){
        if(attribute.type == AttrType::TypeInt){
            cur += sizeof(int)+sizeof(RID)+sizeof(unsigned);
        }
        else if(attribute.type == AttrType::TypeReal){
            cur += sizeof(float)+sizeof(RID)+sizeof(unsigned);
        }else{
            unsigned strLen = *(unsigned *)(cur);
            cur += strLen+sizeof(RID)+sizeof(unsigned);
        }
    }

    //Create a new index node called N2.Here 'the middle point' is not included in N2,which is a big difference with splitDataEntry.
    char *start = cur;
    if(attribute.type == AttrType::TypeInt){
        start += sizeof(int)+sizeof(RID)+sizeof(unsigned);
    }
    else if(attribute.type == AttrType::TypeReal){
        start += sizeof(float)+sizeof(RID)+sizeof(unsigned);
    }else{
        unsigned strLen = *(unsigned *)start;
        start += strLen+sizeof(RID)+sizeof(unsigned);
    }

    memcpy(newn,start,spaceToBeSplit-(start-data));
    *(unsigned *)(newn+PAGE_SIZE-sizeof(unsigned)) = spaceToBeSplit-(start-data); //Set free space indicator for N2
    *(unsigned *)(newn+PAGE_SIZE-2*sizeof(unsigned)) = 0;
    //After deletion there would be unused page, a linked list for these unused page remains to be implemented.
    int rc = ixFileHandle.appendPage(newn);
    if(rc != 0)
        return -1;

    //Get new child entry
    int iLen;
    resolveNewChildEntry(cur,newChildEntry,attribute,iLen);
    newChildEntry.pageNum = ixFileHandle.getNumberOfPages()-1;

    //Reset unused space to 0.Note that we should avoid reset the last 2 unsigned.
    //memset(page,0,PAGE_SIZE-(page-index)-2*sizeof(unsigned));

    //Modify N
    memcpy(index,data,cur-data); //Modify data entry within N
    *(unsigned *)(index+PAGE_SIZE-sizeof(unsigned)) = cur-data; //Set free space indicator for N


    return 0;
}

RC IndexManager::splitDataEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,
    const unsigned insertOffset,char *leaf,const char *composite,const unsigned ckLen,const unsigned pageNumber){
    //Insert entry into the leaf node
    //Insert should be applied to 'leaf' page
    char data[2*PAGE_SIZE];
    char newn[PAGE_SIZE]; //Allocate for new node
    unsigned freeSpaceOffset = *(unsigned *)(leaf+PAGE_SIZE-sizeof(unsigned));
    unsigned spaceToBeSplit = freeSpaceOffset+ckLen;
    memcpy(data,leaf,insertOffset);
    memcpy(data+insertOffset,composite,ckLen);
    memcpy((data+insertOffset+ckLen),leaf+insertOffset,spaceToBeSplit-insertOffset);
    
    //Search for 'the middle point'
    char *cur = data;
    unsigned spaceLeft = 0;
    while(spaceLeft < spaceToBeSplit/2.0){
        if(attribute.type == AttrType::TypeInt){
            cur += sizeof(int)+sizeof(RID);
        }
        else if(attribute.type == AttrType::TypeReal){
            cur += sizeof(float)+sizeof(RID);
        }else{
            unsigned strLen = *(unsigned *)(cur);
            cur += strLen+sizeof(RID)+sizeof(unsigned);
        }
    }

    //create a new leaf node called L2
    memcpy(newn,cur,spaceToBeSplit-(cur-data));
    *(unsigned *)(newn+PAGE_SIZE-sizeof(unsigned)) = spaceToBeSplit-(cur-data); //Set free space indicator for N2
    *(unsigned *)(newn+PAGE_SIZE-2*sizeof(unsigned)) = 1;
    *(unsigned *)(newn+PAGE_SIZE-3*sizeof(unsigned)) = pageNumber;
    //After deletion there would be unused page, a linked list for these unused page remains to be implemented.
    int rc = ixFileHandle.appendPage(newn);
    if(rc != 0)
        return -1;

    //(key,rid) pairs should be copied because there might be dulplicates.
    int iLen;
    resolveNewChildEntry(cur,newChildEntry,attribute,ixFileHandle.getNumberOfPages()-1,iLen);

    //Reset unused space to 0.Note that we should avoid reset the last 2 unsigned.
    //memset(page,0,PAGE_SIZE-(page-leaf)-2*sizeof(unsigned));

    //Modify L
    memcpy(leaf,data,cur-data); //Modify data entry within L
    *(unsigned *)(leaf+PAGE_SIZE-sizeof(unsigned)) = cur-data; //Set free space indicator
    *(unsigned *)(leaf+PAGE_SIZE-3*sizeof(unsigned)) = newChildEntry.pageNum; //Set right sibling to be L2

    return 0;  
}

/**
Big challenge here: assume we have to backtrace to split pages,if we have already written back some of these pages,
but fail to write back certain page in some level of the tree, then there would be inconsistency among pages!
**/
RC backtraceInsert(IXFileHandle &ixFileHandle,const unsigned pageNumber,const Attribute &attribute,
    const void *key,const RID &rid,indexEntry &newChildEntry){
    int ival = 0;//pn is page number to be searched in the next level of B+ tree
    float fval = 0;
    string str;
    unsigned insertLen = 0;
    byte page[PAGE_SIZE];
    int rc = ixFileHandle.readPage(pageNumber,page);
    if(rc != 0)
        return -1;

    dataEntry keyEntry;
    unsigned ckLen;
    transformKeyRIDPair(attribute,keyEntry,key,rid,ckLen);

    unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));
    bool isLeaf = *(unsigned *)(page+PAGE_SIZE-2*sizeof(unsigned));

    if(isLeaf){
        //Leaf node case
        char composite[PAGE_SIZE];
        getCompositeKey(composite,attribute,keyEntry,ckLen);
        bool isFull = true;
        isFull = freeSpaceOffset+ckLen > PAGE_SIZE-4*sizeof(unsigned);        

        unsigned offset;
        searchEntry(ixFileHandle,attribute,keyEntry,page,offset);

        if(!isFull){
            //Since there are enough space,just insert.
            if(offset < freeSpaceOffset)
                memmove(page+offset+ckLen,page+offset,freeSpaceOffset-offset);
            memcpy(page+offset,composite,ckLen);
            *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned)) += ckLen; //Change free space indicator
            //newChildEntry.valid = false;
        }else{
            splitDataEntry(ixFileHandle,newChildEntry,page+offset,page,composite,ckLen,pageNumber);
        }
    }else{
        //Non-leaf node case
        //Search for the right index entry in order to enter the next level of B+ tree
        unsigned offset;
        searchEntry(ixFileHandle,attribute,indexEntry(keyEntry),page,offset);
        
        rc = recursiveInsert(ixFileHandle,*(unsigned *)(page+offset-sizeof(unsigned)),attribute,key,rid,newChildEntry);
        //Backtrace: If there is no split in the lower level,i.e. newChildEntry == NULL,return.
        if(rc != 0)
            return -1;
        else if(!newChildEntry.valid){
            return 0;
        }else{
            //Implement pointer pointing to its child nodes!
            char bin[PAGE_SIZE];
            unsigned iLen;
            getNewChildEntry(bin,newChildEntry,attribute,iLen);
            isFull = freeSpaceOffset+iLen > PAGE_SIZE-4*sizeof(unsigned);

            /**
            If there are enough space in this node for the copy-up entry,just insert it,else split this node.
            Note that either choice requires insert newChildEntry into the page
            TO insert,first search for the insert place,then check if the page has enough space for insertion.
            **/
            unsigned offset;
            searchEntry(ixFileHandle,attribute,newChildEntry,page,offset);

            if(!isFull){ //enough space,just insert
                //Insert
                if(offset < freeSpaceOffset)
                    memmove(page+offset+iLen,page+offset,freeSpaceOffset-offset);
                memcpy(page+offset,bin,iLen);
                *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned)) += iLen; //Change free space indicator
                newChildEntry.valid = false; //Set newChildEntry to be NULL
            }else{ //Since no enough space,split index node
                splitIndexEntry(ixFileHandle,newChildEntry,offset,page,bin,iLen,pageNumber);
                //If this page is root node,then create a new root node
                unsigned rootPageNum;
                ixFileHandle.readRootPointer(rootPageNum);
                if(pageNumber == rootPageNum){
                    unsigned curRoot;
                    rc = createNewRoot(ixFileHandle,newChildEntry,pageNumber,curRoot);
                    if(rc != 0)
                        return -1;
                    rc = ixFileHandle.writeRootPointer(curRoot);
                    if(rc != 0)
                        return -1;
                }
            }
        }
    }
    rc = ixFileHandle.writePage(pageNumber,page);
    if(rc != 0)
        return -1;

    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    //unsigned pn,offset;//page number,offset
    //searchEntry(ixFileHandle,attribute,key,pn,offset);



    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    return -1;
}

RC IX_ScanIterator::close() {
    return -1;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

/**
Read root pointer from hidden page
**/
RC IXFileHandle::readRootPointer(unsigned &root){
    fseek( fp, 4*sizeof(unsigned), SEEK_SET );
    int rc = fread(&root,sizeof(unsigned),1,fp);
    if(rc != 0)
        return -1;
    return 0;
}

RC IXFileHandle::writeRootPointer(const unsigned root){
    fseek( fp, 4*sizeof(unsigned), SEEK_SET );
    int rc = fwrite(&root,sizeof(unsigned),1,fp);
    if(rc != 0)
        return -1;
    return 0;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

