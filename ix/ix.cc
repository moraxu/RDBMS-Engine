#include "ix.h"
#include <iostream>
#include <cstdio>
#include <map>
#include <string>

using namespace std;

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
	return PagedFileManager::instance().createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().openFile(fileName,ixFileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().closeFile(ixFileHandle);
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
For leaf index page,we use Alternative 2,and the page format is:
    [ key value || rid ],...
In addition,
the last 0-3 bytes in a non-leaf index page are used to indicate the first unused byte,
the last 4-7 bytes are used to indicate whether it's a leaf page,
the last 8-11 bytes are used to indicate the page number of its right sibling,
and the last 12-15 bytes are used to indicate the page number of its left sibling.
***/

//Transforms from: void* key ---> dataEntry& de
RC IndexManager::transformKeyRIDPair(const Attribute &attribute,dataEntry &de,const void *key,const RID rid,unsigned &keyLen){
    if(key == NULL) {
        return -1;
    }

    if(attribute.type == AttrType::TypeInt){
        de.ival = *(int *)key;
        keyLen = sizeof(int)+sizeof(RID);
    }
    else if(attribute.type == AttrType::TypeReal){
        de.fval = *(float *)key;
        keyLen = sizeof(float)+sizeof(RID);
    }else{
        unsigned strLen = *(unsigned *)key;
        de.key = string((char *)key+sizeof(unsigned),strLen+sizeof(unsigned));
        keyLen = sizeof(unsigned)+strLen+sizeof(RID);
    }

    de.rid = rid;
    return 0;
}

/**
The following four functions transform between binary stream and dataEntry/indexEntry struct
**/

//Transforms from: BYTE* bin ---> indexEntry& newChildEntry
RC IndexManager::resolveNewChildEntry(char *bin,indexEntry &newChildEntry,
			const Attribute attribute,unsigned &iLen) const {
    char *cur = bin;
    if(attribute.type == AttrType::TypeInt){
        newChildEntry.ival = *(int *)cur;
        cur += sizeof(int);
    }
    else if(attribute.type == AttrType::TypeReal){
        newChildEntry.fval = *(float *)cur;
        cur += sizeof(float);
    }else{
        unsigned strLen = *(unsigned *)cur;
        newChildEntry.key = string((char *)cur+sizeof(unsigned),strLen+sizeof(unsigned));
        cur += strLen+sizeof(unsigned);
    }
    newChildEntry.rid.pageNum = *(unsigned *)cur;
    cur += sizeof(unsigned);
    newChildEntry.rid.slotNum = *(unsigned *)cur;
    cur += sizeof(unsigned);
    newChildEntry.pageNum = *(unsigned *)cur;
    cur += sizeof(unsigned);
    newChildEntry.valid = true;
    iLen = cur-bin;
    return 0;   
}

//Transforms from: indexEntry newChildEntry ---> BYTE* bin
RC IndexManager::getNewChildEntry(char *bin,const indexEntry newChildEntry,const Attribute attribute,unsigned &iLen){
    if(newChildEntry.valid){
        char *cur = bin;
        if(attribute.type == AttrType::TypeInt){
            *(int *)cur = newChildEntry.ival;
            cur += sizeof(int);
        }
        else if(attribute.type == AttrType::TypeReal){
            *(float *)cur = newChildEntry.fval;
            cur += sizeof(float);
        }else{
            unsigned strLen = newChildEntry.key.length();
            *(unsigned*)cur = strLen;
            cur += sizeof(unsigned);
            memcpy(cur,(newChildEntry.key).c_str(),strLen);
            cur += strLen;
        }
        *(unsigned *)cur = newChildEntry.rid.pageNum;
        cur += sizeof(unsigned);
        *(unsigned *)cur = newChildEntry.rid.slotNum;
        cur += sizeof(unsigned);
        *(unsigned *)cur = newChildEntry.pageNum;
        cur += sizeof(unsigned);
        iLen = cur-bin;
        return 0;
    }else{
        iLen = 0;
        return -1;
    }
}

//Transforms from: BYTE* compositeKey ---> dataEntry& de
RC IndexManager::resolveCompositeKey(char *compositeKey,const Attribute &attribute,
			dataEntry &de,unsigned &cLen) const {
    char *composite = compositeKey;
    if(attribute.type == AttrType::TypeInt){
        de.ival = *(int *)composite;
        composite += sizeof(int);
    }
    else if(attribute.type == AttrType::TypeReal){
        de.fval = *(float *)composite;
        composite += sizeof(float);
    }else{
        unsigned strLen = *(unsigned *)composite;
        de.key = string((char *)composite+sizeof(unsigned),strLen+sizeof(unsigned));
        composite += strLen+sizeof(unsigned);
    }
    de.rid.pageNum = *(unsigned *)composite;
    composite += sizeof(unsigned);
    de.rid.slotNum = *(unsigned *)composite;
    composite += sizeof(unsigned);
    cLen = composite-compositeKey;

    return 0;
}

//Transforms from: dataEntry& de ---> BYTE* compositeKey
RC IndexManager::getCompositeKey(char *compositeKey,const Attribute attribute,
		const dataEntry &de,unsigned &cLen){
    char *composite = compositeKey;
    if(attribute.type == AttrType::TypeInt){
        *(int *)composite = de.ival;
        composite += sizeof(int);
    }
    else if(attribute.type == AttrType::TypeReal){
        *(float *)composite = de.fval;
        composite += sizeof(float);
    }else{
        unsigned strLen = de.key.length();
        *(unsigned*)composite = strLen;
        composite += sizeof(unsigned);
        memcpy(composite,(de.key).c_str(),strLen);
        composite += strLen;
    }
    *(unsigned *)composite = de.rid.pageNum;
    composite += sizeof(unsigned);
    *(unsigned *)composite = de.rid.slotNum;
    composite += sizeof(unsigned);
    cLen = composite-compositeKey;

    return 0;
}
/**
End of these five functions.
**/

RC IndexManager::createNewRoot(IXFileHandle &ixFileHandle,const indexEntry &newChildEntry,
		const Attribute attribute,const unsigned leftChild,unsigned &newRootPageNum){
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
            //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(de.ival == target.ival && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
            unsigned slno = de.rid.slotNum;           //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(de.fval == target.fval && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
            //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(de.key == target.key && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
            //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(ie.ival == target.ival && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
            unsigned slno = ie.rid.slotNum;           //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(ie.fval == target.fval && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
            //When key equals and provided rid < rid inside (key,rid) pairs we skip out.
            if(ie.key == target.key && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno > target.rid.slotNum)))
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
RC IndexManager::splitIndexEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,const Attribute attribute,
    unsigned insertOffset,char *index,char *bin,const unsigned iLen,const unsigned pageNumber){
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
        	spaceLeft += sizeof(int)+sizeof(RID)+sizeof(unsigned);
        }
        else if(attribute.type == AttrType::TypeReal){
        	spaceLeft += sizeof(float)+sizeof(RID)+sizeof(unsigned);
        }else{
            unsigned strLen = *(unsigned *)(cur);
            spaceLeft += strLen+sizeof(RID)+sizeof(unsigned);
        }
    }
    cur += spaceLeft;

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
    unsigned newLen;
    resolveNewChildEntry(cur,newChildEntry,attribute,newLen);
    newChildEntry.pageNum = ixFileHandle.getNumberOfPages()-1;

    //Reset unused space to 0.Note that we should avoid reset the last 2 unsigned.
    //memset(page,0,PAGE_SIZE-(page-index)-2*sizeof(unsigned));

    //Modify N
    memcpy(index,data,cur-data); //Modify data entry within N
    *(unsigned *)(index+PAGE_SIZE-sizeof(unsigned)) = cur-data; //Set free space indicator for N


    return 0;
}

RC IndexManager::splitDataEntry(IXFileHandle &ixFileHandle,indexEntry &newChildEntry,const Attribute attribute,
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
        	spaceLeft += sizeof(int)+sizeof(RID);
        }
        else if(attribute.type == AttrType::TypeReal){
        	spaceLeft += sizeof(float)+sizeof(RID);
        }else{
            unsigned strLen = *(unsigned *)(cur);
            spaceLeft += strLen+sizeof(RID)+sizeof(unsigned);
        }
    }
    cur += spaceLeft;

    //create a new leaf node called L2
    memcpy(newn,cur,spaceToBeSplit-(cur-data));
    *(unsigned *)(newn+PAGE_SIZE-sizeof(unsigned)) = spaceToBeSplit-(cur-data); //Set free space indicator for N2
    *(unsigned *)(newn+PAGE_SIZE-2*sizeof(unsigned)) = 1;
    *(unsigned *)(newn+PAGE_SIZE-4*sizeof(unsigned)) = pageNumber;
    unsigned rightSib = *(unsigned *)(leaf+PAGE_SIZE-3*sizeof(unsigned));
    *(unsigned *)(newn+PAGE_SIZE-3*sizeof(unsigned)) = rightSib;
    //After deletion there would be unused page, a linked list for these unused page remains to be implemented.
    int rc = ixFileHandle.appendPage(newn);
    if(rc != 0)
        return -1;

    //(key,rid) pairs should be copied because there might be dulplicates.
    unsigned iLen;
    resolveNewChildEntry(cur,newChildEntry,attribute,iLen);
    newChildEntry.pageNum = ixFileHandle.getNumberOfPages()-1;

    //Change the sibling pointer of the leaf node that N originally points to.
    char r[PAGE_SIZE];
    rc = ixFileHandle.readPage(rightSib, r);
    if(rc != 0)
        return -1;
    *(unsigned *)(r+PAGE_SIZE-4*sizeof(unsigned)) = ixFileHandle.getNumberOfPages()-1;
    rc = ixFileHandle.writePage(rightSib, r);
    if(rc != 0)
        return -1;

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
RC IndexManager::backtraceInsert(IXFileHandle &ixFileHandle,const unsigned pageNumber,const Attribute &attribute,
    const void *key,const RID &rid,indexEntry &newChildEntry){
    bool isFull;
    char page[PAGE_SIZE];
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
            splitDataEntry(ixFileHandle,newChildEntry,attribute,offset,page,composite,ckLen,pageNumber);
        }
    }else{
        //Non-leaf node case
        //Search for the right index entry in order to enter the next level of B+ tree
        unsigned offset;
        searchEntry(ixFileHandle,attribute,indexEntry(keyEntry),page,offset);
        
        rc = backtraceInsert(ixFileHandle,*(unsigned *)(page+offset-sizeof(unsigned)),attribute,key,rid,newChildEntry);
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
                splitIndexEntry(ixFileHandle,newChildEntry,attribute,offset,page,bin,iLen,pageNumber);
                //If this page is root node,then create a new root node
                unsigned rootPageNum;
                ixFileHandle.readRootPointer(rootPageNum);
                if(pageNumber == rootPageNum){
                    unsigned curRoot;
                    rc = createNewRoot(ixFileHandle,newChildEntry,attribute,pageNumber,curRoot);
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
	unsigned root;

	if(ixFileHandle.noPages == 0){
		char firstPage[PAGE_SIZE];
		memset(firstPage,0,sizeof(firstPage));
		*(unsigned *)(firstPage+PAGE_SIZE-sizeof(unsigned)) = 0;
		*(unsigned *)(firstPage+PAGE_SIZE-2*sizeof(unsigned)) = 1;
		*(int *)(firstPage+PAGE_SIZE-3*sizeof(int)) = -1;
		*(int *)(firstPage+PAGE_SIZE-4*sizeof(int)) = -1;
		int rc = ixFileHandle.appendPage(firstPage);
		if(rc != 0)
			return -1;
	}

	int rc = ixFileHandle.readRootPointer(root);
	if(rc != 0)
	    return -1;

	indexEntry newChildEntry;
	rc = backtraceInsert(ixFileHandle, root, attribute, key, rid, newChildEntry);
	if(rc != 0)
	    return -1;

    return 0;
}

RC IndexManager::findFirstLeafPage(IXFileHandle& fileHandle, unsigned& pageNo) {
    if(fileHandle.noPages == 0) {
        return -1;
    }

    unsigned rootPage;
    RC rc = fileHandle.readRootPointer(rootPage);
    if(rc != 0)
        return -1;

    byte page[PAGE_SIZE];
    for(unsigned currPage = rootPage ; true ; currPage = *reinterpret_cast<unsigned*>(page)) {
        rc = fileHandle.readPage(currPage,page);
        if(rc != 0) {
            return -1;
        }

        if(*reinterpret_cast<unsigned*>(page+PAGE_SIZE-2*sizeof(unsigned)) == 1) {
            pageNo = currPage;
            return 0;
        }
    }

    return -1; //it's not even possible to reach this statement..
}

RC IndexManager::searchIndexTree(IXFileHandle& fileHandle, const Attribute& attribute, const dataEntry& dataEnt, unsigned& leafPageNo) {
    if(fileHandle.noPages == 0) {
        return -1;
    }

    unsigned rootPage;
    RC rc = fileHandle.readRootPointer(rootPage);
    if(rc != 0)
        return -1;

    return searchIndexTree(fileHandle, rootPage, attribute, dataEnt,leafPageNo);
}

RC IndexManager::searchIndexTree(IXFileHandle& fileHandle, const unsigned pageNumber, const Attribute& attribute, const dataEntry& dataEnt, unsigned& leafPageNo) {
    char page[PAGE_SIZE];
    RC rc = fileHandle.readPage(pageNumber,page);
    if(rc != 0) {
        return -1;
    }
    //If page is a leaf, return its number
    if(*reinterpret_cast<unsigned*>(page+PAGE_SIZE-2*sizeof(unsigned)) == 1) {
        leafPageNo = pageNumber;
        return 0;
    }
    //If it's not yet a leaf...

    //Check if if it's smaller than the first key on the index page
    indexEntry nextEnt;
    unsigned iLen;
    resolveNewChildEntry(page+sizeof(unsigned), nextEnt, attribute, iLen);
    if(attribute.type == AttrType::TypeInt) {
        if(dataEnt.ival < nextEnt.ival) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page), attribute, dataEnt, leafPageNo);
        }
    }
    else if(attribute.type == AttrType::TypeReal) {
        if(dataEnt.fval < nextEnt.fval) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page), attribute, dataEnt, leafPageNo);
        }
    }
    else {
        if(dataEnt.key < nextEnt.key) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page), attribute, dataEnt, leafPageNo);
        }
    }

    //Else, compare it with every key on the index page
    //searchEntry operates on indexEntry objects so we need to construct such object
    if(attribute.type == AttrType::TypeInt) {
        nextEnt.ival = dataEnt.ival;
    }
    else if(attribute.type == AttrType::TypeReal) {
        nextEnt.fval = dataEnt.fval;
    }
    else {
        nextEnt.key = dataEnt.key;
    }
    unsigned offset;
    rc = searchEntry(fileHandle, attribute, nextEnt, page, offset);
    if(rc != 0) {
        return -1;
    }
    //if offset is at the end of all the entries, it means we have to use the last page pointer
    if(offset == *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned))) {
        return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, leafPageNo);
    }
    //if offset < freeSpaceOffset, then
    //searchEntry's last line say:
    /*
    //Eventually the place where the first data entry >= target entry is found.
    offset = cur-page;
     */
    //So we need to check if:
    // data entry > target (we then use pointer BEFORE offset)
    //  or
    // data entry == target (we then use pointer of the offset's index entry)
    resolveNewChildEntry(page+offset, nextEnt, attribute, iLen);
    if(attribute.type == AttrType::TypeInt) {
        if(dataEnt.ival < nextEnt.ival) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, leafPageNo);
        }
        else {
            return searchIndexTree(fileHandle, nextEnt.pageNum, attribute, dataEnt, leafPageNo);
        }
    }
    else if(attribute.type == AttrType::TypeReal) {
        if(dataEnt.fval < nextEnt.fval) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, leafPageNo);
        }
        else {
            return searchIndexTree(fileHandle, nextEnt.pageNum, attribute, dataEnt, leafPageNo);
        }
    }
    else {
        if(dataEnt.key < nextEnt.key) {
            return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, leafPageNo);
        }
        else {
            return searchIndexTree(fileHandle, nextEnt.pageNum, attribute, dataEnt, leafPageNo);
        }
    }
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
    ix_ScanIterator.setIxFileHandle(ixFileHandle);
    ix_ScanIterator.setAttribute(attribute);

    dataEntry lowKeyEntry;
    dataEntry highKeyEntry;
    unsigned dummyLength; //not needed
    RID dummyRID = { 0, 0 };    //not needed
    if(transformKeyRIDPair(attribute, lowKeyEntry, lowKey, dummyRID, dummyLength) != 0) {
        ix_ScanIterator.setLowKeyInfinity(true);
    }
    else {
        ix_ScanIterator.setLowKeyEntry(lowKeyEntry);
    }
    if(transformKeyRIDPair(attribute, highKeyEntry, highKey, dummyRID, dummyLength) != 0) {
        ix_ScanIterator.setHighKeyInfinity(true);
    }
    else {
        ix_ScanIterator.setHighKeyEntry(highKeyEntry);
    }

    ix_ScanIterator.setLowKeyInclusive(lowKeyInclusive);
    ix_ScanIterator.setHighKeyInclusive(highKeyInclusive);

    ix_ScanIterator.setScanStarted(false);
    ix_ScanIterator.setEnteredNewPage(true); //although it doesn't practically matter at this point..

    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
	unsigned root;
	int rc = ixFileHandle.readRootPointer(root);
	if(rc != 0)
		return;

	printNode(ixFileHandle,attribute,root,0);
	return;
}

void IndexManager::printTab(const unsigned level) const {
	for(int i = 0;i < level;i++)
		cout<<"\t";
}

string IndexManager::RIDtoStr(const RID &rid) const {
	string ridStr;
	ridStr += "(";
	ridStr += to_string(rid.pageNum);
	ridStr += ",";
	ridStr += to_string(rid.slotNum);
	ridStr += ")";
	return ridStr;
}

void IndexManager::printNode(IXFileHandle &ixFileHandle, const Attribute &attribute,
		const unsigned pageNumber,const unsigned level) const {
	char node[PAGE_SIZE];
	int rc = ixFileHandle.readPage(pageNumber, node);
	if(rc != 0)
		return;

	char *cur = node;
	unsigned isLeaf = *(unsigned *)(node+PAGE_SIZE-2*sizeof(unsigned));
	unsigned freeSpaceOffset = *(unsigned *)(node+PAGE_SIZE-sizeof(unsigned));
	printTab(level);
	cout<<"{\"keys\":[";
	if(isLeaf){
		map<string,vector<string> > dn;
		while(cur-node < freeSpaceOffset){
			dataEntry dEntry;
			unsigned cLen;
			resolveCompositeKey(cur, attribute, dEntry, cLen);
			if(attribute.type == AttrType::TypeInt){
				dn[to_string(dEntry.ival)].push_back(RIDtoStr(dEntry.rid));
			}
			else if(attribute.type == AttrType::TypeReal){
				dn[to_string(dEntry.fval)].push_back(RIDtoStr(dEntry.rid));
			}else{
				dn[dEntry.key].push_back(RIDtoStr(dEntry.rid));
			}
		}
		for(auto it = dn.begin();it != dn.end();){
			cout<<"\""<<it->first<<":[";
			for(int i = 0;i < it->second.size();i++){
				cout<<it->second[i];
				if(i < it->second.size()-1) cout<<",";
			}
			cout<<"]\"";
			if(++it != dn.end()) cout<<",";
		}
		cout<<"]}";
		return;
	}else{
		vector<unsigned> pointers;
		while(cur-node < freeSpaceOffset){
			indexEntry iEntry;
			unsigned iLen;
			resolveNewChildEntry(cur, iEntry, attribute, iLen);
			pointers.push_back(iEntry.pageNum);
			cout<<"\""<<iEntry.key<<"\"";
			cur += iLen;
			if(cur-node < freeSpaceOffset){
				cout<<",";
			}
		}
		cout<<"],"<<endl;
		printTab(level);
		cout<<" \"children\": ["<<endl;
		for(int i = 0;i < pointers.size();i++){
			printNode(ixFileHandle,attribute,pointers[i],level+1);
			if(i != pointers.size()-1)
				cout<<","<<endl;
		}
		cout<<endl<<"]}";
	}
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::determineInitialPageAndOffset() {
    RC rc;
    if(lowKeyInfinity) {
        rc = IndexManager::instance().findFirstLeafPage(ixFileHandle, currPage);
        if(rc != 0) {
            return rc;
        }
        currOffset = 0;
        return 0;
    }
    else {
        rc = IndexManager::instance().searchIndexTree(ixFileHandle, attribute,lowKeyEntry, currPage);
        if(rc != 0) {
            return rc;
        }
    }

    char page[PAGE_SIZE];
    rc = ixFileHandle.readPage(currPage,page);
    if(rc != 0) {
        return rc;
    }
    rc = IndexManager::instance().searchEntry(ixFileHandle, attribute, lowKeyEntry, page, currOffset);
    if(rc != 0) {
        return rc;
    }
    //If the record is not on the page where it's supposed to be,
    //getNextEntry() will set "currOffset" and "currPage" to the beginning of next page, if any
    if(currOffset == *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned))) {
        return 0;
    }
    //if offset < freeSpaceOffset, then
    //searchEntry's last line say:
    /*
    //Eventually the place where the first data entry >= target entry is found.
    offset = cur-page;
     */
    //So we need to check if:
    // data entry > target OR (data entry == target AND lowKeyInclusive == true) --> (we then start from this offset)
    //  or
    // data entry == target AND lowKeyInclusive == false --> (we then start from next offset, OR next page if it was the last data entry on the page)
    dataEntry readDataEntry;
    unsigned readDataEntryLen;
    IndexManager::instance().resolveCompositeKey(page+currOffset, attribute, readDataEntry, readDataEntryLen);
    if(attribute.type == AttrType::TypeInt) {
        if(lowKeyEntry.ival < readDataEntry.ival || (lowKeyEntry.ival == readDataEntry.ival && lowKeyInclusive)) {
            //we start from current page and offset; those values are already stored in currPage and currOffset fields
            return 0;
        }
        else if(lowKeyEntry.ival == readDataEntry.ival && !lowKeyInclusive){
            //we start from next offset
            //(if it was the last data entry on the page, getNextEntry() will set
            //"currOffset" and "currPage" to the beginning of next page, if any)
            currOffset += readDataEntryLen;
            return 0;
        }
        else {
            //some kind of unexpected error, I am not even sure if entering this 'else' block is ever possible
            return -1;
        }
    }
    else if(attribute.type == AttrType::TypeReal) {
        if(lowKeyEntry.fval < readDataEntry.fval || (lowKeyEntry.fval == readDataEntry.fval && lowKeyInclusive)) {
            //we start from current page and offset; those values are already stored in currPage and currOffset fields
            return 0;
        }
        else if(lowKeyEntry.fval == readDataEntry.fval && !lowKeyInclusive){
            //we start from next offset
            //(if it was the last data entry on the page, getNextEntry() will set
            //"currOffset" and "currPage" to the beginning of next page, if any)
            currOffset += readDataEntryLen;
            return 0;
        }
        else {
            //some kind of unexpected error, I am not even sure if entering this 'else' block is ever possible
            return -1;
        }
    }
    else {
        if(lowKeyEntry.key < readDataEntry.key || (lowKeyEntry.key == readDataEntry.key && lowKeyInclusive)) {
            //we start from current page and offset; those values are already stored in currPage and currOffset fields
            return 0;
        }
        else if(lowKeyEntry.key == readDataEntry.key && !lowKeyInclusive){
            //we start from next offset
            //(if it was the last data entry on the page, getNextEntry() will set
            //"currOffset" and "currPage" to the beginning of next page, if any)
            currOffset += readDataEntryLen;
            return 0;
        }
        else {
            //some kind of unexpected error, I am not even sure if entering this 'else' block is ever possible
            return -1;
        }
    }
}

void* IX_ScanIterator::transformDataEntryKey(dataEntry dataEnt, void* key) const {
    std::vector<byte> bytesToWrite;

    if(attribute.type == AttrType::TypeInt) {
        const byte* ivalPtr = reinterpret_cast<const byte*>(&dataEnt.ival);
        bytesToWrite.insert(bytesToWrite.end(), ivalPtr, ivalPtr + sizeof(dataEnt.ival));
    }
    else if(attribute.type == AttrType::TypeReal) {
        const byte* fvalPtr = reinterpret_cast<const byte*>(&dataEnt.fval);
        bytesToWrite.insert(bytesToWrite.end(), fvalPtr, fvalPtr + sizeof(dataEnt.fval));
    }
    else {
        unsigned keyLength = dataEnt.key.length();
        const byte* keyLengthPtr = reinterpret_cast<const byte*>(&keyLength);
        const byte* keyPtr = reinterpret_cast<const byte*>(dataEnt.key.c_str());
        bytesToWrite.insert(bytesToWrite.end(), keyLengthPtr, keyLengthPtr + sizeof(keyLength));
        bytesToWrite.insert(bytesToWrite.end(), keyPtr, keyPtr + keyLength);
    }

    memcpy(key, bytesToWrite.data(), bytesToWrite.size());
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    //During the very first scan operation we need to find the corresponding page and offset
    //of the first qualifying data entry
    if(!scanStarted) {
        if(determineInitialPageAndOffset() != 0) {
            return IX_EOF;
        }
    }
    //At this point, currPage and currOffset fields point to a data entry that fulfills lowKey condition.
    //We now process records until we encounter a data entry that doesn't fulfill highKey condition.

    //-----------------------------------------------------------------------------------------------------------
    //IMPORTANT NOTE regarding simultaneous deletion; from project 3 description:
    /*
     "While making an index scan work correctly for selection, join, and non-index-key update operations is
     relatively straightforward, deletion operations are more complicated, even when using the simplified
     approaches to deletion described above. You must ensure that it is possible to use an index scan to find
     and then delete all index entries satisfying a condition. That is, the following client code segment should work:"

     IX_ScanIterator ix_ScanIterator;
     indexManager.scan(ixFileHandle, ..., ix_ScanIterator);
     while ((rc = ix_ScanIterator.getNextEntry(rid, &key)) != IX_EOF)
     {
         indexManager.deleteEntry(ixFileHandle, attribute, &key, rid);
     }
     */
     //That's why we have to include two additional fields in IX_ScanIterator: lastReadFreeSpaceOffset and lastReadDataEntryLength
     //And then:
     //if currentFreeSpaceOffset < lastReadFreeSpaceOffset
     //     currOffset = currOffset - lastReadDataEntryLength;
     //because the last removed data entry caused the remaining ones to be shifted "lastReadDataEntryLength" bytes
     //towards the beginning of the page
    //-----------------------------------------------------------------------------------------------------------

    char page[PAGE_SIZE];
    RC rc = ixFileHandle.readPage(currPage,page);
    if(rc != 0) {
        return rc;
    }
    unsigned currentFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));

    //If it's time to skip to the next page, if any
    // (it's a loop because theoretically there might be empty an empty page in the middle)
    while(currOffset >= currentFreeSpaceOffset) {
        int nextPage = *reinterpret_cast<int *>(page+PAGE_SIZE-3*sizeof(unsigned));
        if(nextPage == -1) {
            return IX_EOF;
        }
        //There is next page
        rc = ixFileHandle.readPage(currPage,page);
        if(rc != 0) {
            return rc;
        }
        currentFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));

        currPage = nextPage;
        currOffset = 0;
        enteredNewPage = true;
    }

    if(scanStarted && !enteredNewPage) {
        if(currentFreeSpaceOffset < lastReadFreeSpaceOffset) {
            currOffset = currOffset - lastReadDataEntryLength;
        }
    }
    else {
        scanStarted = true;
        enteredNewPage = false;
        lastReadFreeSpaceOffset = currentFreeSpaceOffset;
    }

    dataEntry readDataEntry;
    IndexManager::instance().resolveCompositeKey(page+currOffset, attribute, readDataEntry, lastReadDataEntryLength);
    if(attribute.type == AttrType::TypeInt) {
        if(!highKeyInfinity && ((highKeyEntry.ival < readDataEntry.ival) || (highKeyEntry.ival == readDataEntry.ival && !highKeyInclusive))) {
            return IX_EOF;
        }
    }
    else if(attribute.type == AttrType::TypeReal) {
        if(!highKeyInfinity && ((highKeyEntry.fval < readDataEntry.fval) || (highKeyEntry.fval == readDataEntry.fval && !highKeyInclusive))) {
            return IX_EOF;
        }
    }
    else {
        if(!highKeyInfinity && ((highKeyEntry.key < readDataEntry.key) || (highKeyEntry.key == readDataEntry.key && !highKeyInclusive))) {
            return IX_EOF;
        }
    }

    rid.pageNum = readDataEntry.rid.pageNum;
    rid.slotNum = readDataEntry.rid.slotNum;
    transformDataEntryKey(readDataEntry, key);
    currOffset += lastReadDataEntryLength;
    return 0;
}

RC IX_ScanIterator::close() {
    return PagedFileManager::instance().closeFile(ixFileHandle);
}

const IXFileHandle &IX_ScanIterator::getIxFileHandle() const {
    return ixFileHandle;
}

void IX_ScanIterator::setIxFileHandle(const IXFileHandle &ixFileHandle) {
    IX_ScanIterator::ixFileHandle = ixFileHandle;
}

const Attribute &IX_ScanIterator::getAttribute() const {
    return attribute;
}

void IX_ScanIterator::setAttribute(const Attribute &attribute) {
    IX_ScanIterator::attribute = attribute;
}

bool IX_ScanIterator::isLowKeyInclusive() const {
    return lowKeyInclusive;
}

void IX_ScanIterator::setLowKeyInclusive(bool lowKeyInclusive) {
    IX_ScanIterator::lowKeyInclusive = lowKeyInclusive;
}

bool IX_ScanIterator::isHighKeyInclusive() const {
    return highKeyInclusive;
}

void IX_ScanIterator::setHighKeyInclusive(bool highKeyInclusive) {
    IX_ScanIterator::highKeyInclusive = highKeyInclusive;
}

const dataEntry &IX_ScanIterator::getLowKeyEntry() const {
    return lowKeyEntry;
}

void IX_ScanIterator::setLowKeyEntry(const dataEntry &lowKeyEntry) {
    IX_ScanIterator::lowKeyEntry = lowKeyEntry;
}

const dataEntry &IX_ScanIterator::getHighKeyEntry() const {
    return highKeyEntry;
}

void IX_ScanIterator::setHighKeyEntry(const dataEntry &highKeyEntry) {
    IX_ScanIterator::highKeyEntry = highKeyEntry;
}

bool IX_ScanIterator::isLowKeyInfinity() const {
    return lowKeyInfinity;
}

void IX_ScanIterator::setLowKeyInfinity(bool lowKeyInfinity) {
    IX_ScanIterator::lowKeyInfinity = lowKeyInfinity;
}

bool IX_ScanIterator::isHighKeyInfinity() const {
    return highKeyInfinity;
}

void IX_ScanIterator::setHighKeyInfinity(bool highKeyInfinity) {
    IX_ScanIterator::highKeyInfinity = highKeyInfinity;
}

bool IX_ScanIterator::isScanStarted() const {
    return scanStarted;
}

void IX_ScanIterator::setScanStarted(bool scanStarted) {
    IX_ScanIterator::scanStarted = scanStarted;
}

bool IX_ScanIterator::isEnteredNewPage() const {
    return enteredNewPage;
}

void IX_ScanIterator::setEnteredNewPage(bool enteredNewPage) {
    IX_ScanIterator::enteredNewPage = enteredNewPage;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    noPages = 0;
    fp = nullptr;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){
	//Intend to read a non-existing page!
	//Fix test case 06
	if(pageNum >= noPages){
		ixReadPageCounter++;
		return -1;
	}

	//NOTE:every time when a file is ready for write or read, the file pointer starts from sizeof(int)*4, NOT 0.
	fseek( fp, (pageNum+1)*PAGE_SIZE, SEEK_SET );
	unsigned res = fread(data,sizeof(char),PAGE_SIZE,fp);
	//File read error!
	if(res != PAGE_SIZE && feof(fp) != EOF)
		return -1;

	ixReadPageCounter++;
	return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
	//Intend to update a non-existing page!
	//Fix test case 06
	if(pageNum >= noPages){
		ixWritePageCounter++;
		return -1;
	}

	//NOTE:every time when a file is ready for write or read, the file pointer starts from sizeof(int)*4, NOT 0.
	fseek( fp, (pageNum+1)*PAGE_SIZE, SEEK_SET );
	unsigned res = fwrite(data,sizeof(char),PAGE_SIZE,fp);
	//File write error!
	if(res != PAGE_SIZE)
		return -1;

	ixWritePageCounter++;
	return 0;
}

RC IXFileHandle::appendPage(const void *data){
	fseek( fp, 0, SEEK_END );
	unsigned res = fwrite(data,sizeof(char),PAGE_SIZE,fp);
	//File append error!
	if(res != PAGE_SIZE)
		return -1;

	noPages++;
	ixAppendPageCounter++;
	return 0;
}

/**
Hidden page format:
    [ ixReadPageCounter = 0 || ixWritePageCounter = 0 || ixAppendPageCounter = 0
    || number of pages || dummy node list pointing to the root of indexes ]
This function reads root pointer from hidden page.
**/
RC IXFileHandle::readRootPointer(unsigned &root){
    fseek( fp, 4*sizeof(unsigned), SEEK_SET );
    unsigned rc = fread(&root,sizeof(unsigned),1,fp);
    if(rc != 1)
        return -1;

    ++ixReadPageCounter;
    return 0;
}

RC IXFileHandle::writeRootPointer(const unsigned root){
    fseek( fp, 4*sizeof(unsigned), SEEK_SET );
    unsigned rc = fwrite(&root,sizeof(unsigned),1,fp);
    if(rc != 1)
        return -1;

    ++ixWritePageCounter;
    return 0;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

