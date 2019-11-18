#include "ix.h"
#include <iostream>
#include <cstdio>
#include <map>
#include <string>
#include <unistd.h>

using namespace std;

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

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
	//cout<<cnt[0]<<" "<<cnt[1]<<" "<<cnt[2]<<" "<<cnt[3]<<endl;
	fseek(fp,0,SEEK_SET);
	fwrite(cnt,1,PAGE_SIZE,fp);
	//if fp is not closed,there could be undefined behaviors, e.g.  counter value undefined.
	//This is because when two file descriptors exist concurrently, they read like the other never writes.(they read original data)
	fclose(fp);
	return 0;
}

RC IndexManager::destroyFile(const std::string &fileName) {
	return remove(fileName.c_str());
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
	//File doesn't exist!
	if(access(fileName.c_str(),F_OK)) {
		return -1;
	}

	//fileHandle is already a handle for some open file!
	if(ixFileHandle.fp) {
		return -1;
	}

	ixFileHandle.fp = fopen(fileName.c_str(),"rb+");
	//File open error!
	if(!ixFileHandle.fp) {
		return -1;
	}

	unsigned cnt[6];
	fseek(ixFileHandle.fp,0,SEEK_SET);
	fread(cnt,sizeof(unsigned),6,ixFileHandle.fp);
	ixFileHandle.ixReadPageCounter = cnt[0];
	ixFileHandle.ixWritePageCounter =  cnt[1];
	ixFileHandle.ixAppendPageCounter = cnt[2];
	ixFileHandle.noPages = cnt[3];
	ixFileHandle.lastTableID = cnt[4];
	ixFileHandle.rootPage = cnt[5];

	//cout<<cnt[0]<<" "<<cnt[1]<<" "<<cnt[2]<<" "<<cnt[3]<<endl;
	return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
	if(ixFileHandle.fp == NULL) {
		return -1;
	}
	fseek(ixFileHandle.fp, 0, SEEK_SET);
	unsigned cnt[6];
	cnt[0] = ixFileHandle.ixReadPageCounter;
	cnt[1] = ixFileHandle.ixWritePageCounter;
	cnt[2] = ixFileHandle.ixAppendPageCounter;
	cnt[3] = ixFileHandle.noPages;
	cnt[4] = ixFileHandle.lastTableID;
	cnt[5] = ixFileHandle.rootPage;
	fwrite(cnt,sizeof(unsigned),6,ixFileHandle.fp);
	//File close error!
	if(fclose(ixFileHandle.fp) == EOF) {
		return -1;
	}
	ixFileHandle.fp = NULL;
	return 0;
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
        de.key = string((char *)key+sizeof(unsigned),strLen);
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
        newChildEntry.key = string(cur+sizeof(unsigned),strLen);
        //cout<<newChildEntry.key<<endl;
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
        de.key = string(composite+sizeof(unsigned),strLen);
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
End of these four functions.
**/

RC IndexManager::createNewRoot(IXFileHandle &ixFileHandle,const indexEntry &newChildEntry,
		const Attribute attribute,const unsigned leftChild,unsigned &newRootPageNum){
    char root[PAGE_SIZE];
    char *cur = root;

    //copy the left child node pointer into new root node
    *(unsigned *)cur = leftChild;
    cur += sizeof(unsigned);

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
            /*
			 * When key equals and data entry rid <= target rid we skip out of loop.
			 * For search,insert and delete, (key,RID) pair in data entry could equal that in target entry.
			 * For insert this is invalid, while for search and delete this is valid.
			 */
            if(de.ival == target.ival && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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
            unsigned slno = de.rid.slotNum;
            if(de.fval == target.fval && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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
RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
		const indexEntry &target,char *page,unsigned &offset){
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
            /*
             * When key equals and index entry rid <= target rid we skip out of loop.
             * For search,insert and delete, (key,RID) pair in index entry could equal that in target entry.
             * For insert this is invalid, while for search and delete this is valid.
             */
            if(ie.ival == target.ival && (pno > target.rid.pageNum ||
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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
                    (pno == target.rid.pageNum && slno >= target.rid.slotNum)))
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

RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
		const indexEntry &target,const bool lowKeyInclusive,char *page,unsigned &offset){
	if(lowKeyInclusive)
		return searchEntry(ixFileHandle, attribute, target, page, offset);
	else{
		unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));

		//Skip the first node pointer
		char *cur = page+sizeof(unsigned);
		indexEntry ie;
		unsigned iLen;
		if(attribute.type == AttrType::TypeInt){
			if(cur-page < freeSpaceOffset)
				resolveNewChildEntry(cur,ie,attribute,iLen);
			while(cur-page < freeSpaceOffset && ie.ival <= target.ival){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveNewChildEntry(cur,ie,attribute,iLen);
			}
		}else if(attribute.type == AttrType::TypeReal){
			if(cur-page < freeSpaceOffset)
				resolveNewChildEntry(cur,ie,attribute,iLen);
			while(cur-page < freeSpaceOffset && ie.fval <= target.fval){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveNewChildEntry(cur,ie,attribute,iLen);
			}
		}else{
			if(cur-page < freeSpaceOffset)
				resolveNewChildEntry(cur,ie,attribute,iLen);
			while(cur-page < freeSpaceOffset && ie.key <= target.key){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveNewChildEntry(cur,ie,attribute,iLen);
			}
		}
		//Eventually the place where the first data entry >= target entry is found.
		offset = cur-page;

		return 0;
	}
}

RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                   const dataEntry &target,const bool lowKeyInclusive,char *page,unsigned &offset){
	if(lowKeyInclusive)
		return searchEntry(ixFileHandle, attribute, target, page, offset);
	else{
		unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));

		char *cur = page;
		dataEntry de;
		unsigned iLen;
		if(attribute.type == AttrType::TypeInt){
			if(cur-page < freeSpaceOffset)
				resolveCompositeKey(cur,attribute,de,iLen);
			while(cur-page < freeSpaceOffset && de.ival <= target.ival){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveCompositeKey(cur,attribute,de,iLen);
			}
		}else if(attribute.type == AttrType::TypeReal){
			if(cur-page < freeSpaceOffset)
				resolveCompositeKey(cur,attribute,de,iLen);
			while(cur-page < freeSpaceOffset && de.fval <= target.fval){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveCompositeKey(cur,attribute,de,iLen);
			}
		}else{
			if(cur-page < freeSpaceOffset)
				resolveCompositeKey(cur,attribute,de,iLen);
			while(cur-page < freeSpaceOffset && de.key <= target.key){
				cur += iLen;
				if(cur-page < freeSpaceOffset)
					resolveCompositeKey(cur,attribute,de,iLen);
			}
		}
		//Eventually the offset where the first data entry >= target entry is found.
		offset = cur-page;

		return 0;
	}
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
    memcpy(data+insertOffset+iLen,index+insertOffset,freeSpaceOffset-insertOffset);
    
    //Search for 'the middle point'
    //The search process can be omptimized by starting from the insert place
    char *cur = data;
	unsigned dataLen = 0;
	cur += sizeof(unsigned);
	while(cur+dataLen-data < spaceToBeSplit/2.0){
		cur += dataLen;
		if(attribute.type == AttrType::TypeInt){
			dataLen = sizeof(int)+sizeof(RID)+sizeof(unsigned);
		}
		else if(attribute.type == AttrType::TypeReal){
			dataLen = sizeof(float)+sizeof(RID)+sizeof(unsigned);
		}else{
			unsigned strLen = *(unsigned *)(cur);
			dataLen = strLen+sizeof(RID)+2*sizeof(unsigned);
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
        start += strLen+sizeof(RID)+2*sizeof(unsigned);
    }

    memcpy(newn,start-sizeof(unsigned),sizeof(unsigned));
    memcpy(newn+sizeof(unsigned),start,spaceToBeSplit-(start-data));
    *(unsigned *)(newn+PAGE_SIZE-sizeof(unsigned)) = spaceToBeSplit-(start-data); //Set free space indicator for N2
    *(unsigned *)(newn+PAGE_SIZE-2*sizeof(unsigned)) = 0;
    //After deletion there would be unused page, a linked list for these unused page remains to be implemented.
    int rc = ixFileHandle.appendPage(newn);
    //cout<<"Append N2 return "<<rc<<endl;
    if(rc != 0)
        return -1;

    //Get new child entry
    unsigned newLen;
    rc = resolveNewChildEntry(cur,newChildEntry,attribute,newLen);
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
    memcpy((data+insertOffset+ckLen),leaf+insertOffset,freeSpaceOffset-insertOffset);
    
    //Search for 'the middle point'
    char *cur = data;
    unsigned dataLen = 0;
    while(cur+dataLen-data < spaceToBeSplit/2.0){
    	cur += dataLen;
        if(attribute.type == AttrType::TypeInt){
        	dataLen = sizeof(int)+sizeof(RID);
        }
        else if(attribute.type == AttrType::TypeReal){
        	dataLen = sizeof(float)+sizeof(RID);
        }else{
            unsigned strLen = *(unsigned *)(cur);
            dataLen = strLen+sizeof(RID)+sizeof(unsigned);
        }
    }
    cout<<"insertOffset:"<<insertOffset<<" spaceToBeSplit:"<<spaceToBeSplit<<" half page offset:"<<cur-data<<endl;

    //create a new leaf node called L2
    memcpy(newn,cur,spaceToBeSplit-(cur-data));
    *(unsigned *)(newn+PAGE_SIZE-sizeof(unsigned)) = spaceToBeSplit-(cur-data); //Set free space indicator for N2
    *(unsigned *)(newn+PAGE_SIZE-2*sizeof(unsigned)) = 1;
    *(unsigned *)(newn+PAGE_SIZE-4*sizeof(unsigned)) = pageNumber;
    //rightSib could be -1 if it's the rightmost page
    int rightSib = *(int *)(leaf+PAGE_SIZE-3*sizeof(unsigned));
    *(int *)(newn+PAGE_SIZE-3*sizeof(unsigned)) = rightSib;
    //After deletion there would be unused page, a linked list for these unused page remains to be implemented.
    int rc = ixFileHandle.appendPage(newn);
    //cout<<"Append L2 return "<<rc<<endl;
    if(rc != 0)
        return -1;

    //(key,rid) pairs should be copied because there might be dulplicates.
    unsigned iLen;
    resolveNewChildEntry(cur,newChildEntry,attribute,iLen);
    newChildEntry.pageNum = ixFileHandle.getNumberOfPages()-1;

    //Change the sibling pointer of the leaf node that N originally points to.
    cout<<"Right sibling page number:"<<rightSib<<endl;
    if(rightSib != -1){
    	char r[PAGE_SIZE];
		rc = ixFileHandle.readPage(rightSib, r);
		//cout<<"Read right sibling return "<<rc<<endl;
		if(rc != 0)
			return -1;
		*(unsigned *)(r+PAGE_SIZE-4*sizeof(unsigned)) = ixFileHandle.getNumberOfPages()-1;
		rc = ixFileHandle.writePage(rightSib, r);
		//cout<<"Modify right sibling return "<<rc<<endl;
		if(rc != 0)
			return -1;

    }
    //cout<<"End of modifying right sibling."<<endl;

    //Reset unused space to 0.Note that we should avoid reset the last 2 unsigned.
    //memset(page,0,PAGE_SIZE-(page-leaf)-2*sizeof(unsigned));

    //Modify L
    memcpy(leaf,data,cur-data); //Modify data entry within L
    *(unsigned *)(leaf+PAGE_SIZE-sizeof(unsigned)) = cur-data; //Set free space indicator
    *(unsigned *)(leaf+PAGE_SIZE-3*sizeof(unsigned)) = newChildEntry.pageNum; //Set right sibling to be L2

    /*
     * If we are splitting the first page of an index file(which is root page as well as data entry page),
     * we should create the first index root page manually because no backtrace happens.
    */
    //cout<<"Current root page number:"<<ixFileHandle.rootPage<<" current data entry page:"<<pageNumber<<endl;
    if(ixFileHandle.rootPage == pageNumber){
    	rc = createNewRoot(ixFileHandle, newChildEntry, attribute, pageNumber, newChildEntry.pageNum);
    	if(rc != 0)
    	    return -1;
    	ixFileHandle.rootPage = newChildEntry.pageNum;
    	//cout<<"Root page number:"<<ixFileHandle.rootPage<<endl;
    }

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
    //cout<<rid.pageNum<<"th data entry length(when insertion):"<<ckLen<<endl;

    unsigned freeSpaceOffset = *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned));
    bool isLeaf = *(unsigned *)(page+PAGE_SIZE-2*sizeof(unsigned));

    if(isLeaf){
        //Leaf node case
        char composite[PAGE_SIZE];
        getCompositeKey(composite,attribute,keyEntry,ckLen);
        isFull = freeSpaceOffset+ckLen > PAGE_SIZE-4*sizeof(unsigned);        

        unsigned offset;
        rc = searchEntry(ixFileHandle,attribute,keyEntry,page,offset);
        if(rc != 0)
            return -1;

        if(!isFull){
            //Since there are enough space,just insert.
            if(offset < freeSpaceOffset)
                memmove(page+offset+ckLen,page+offset,freeSpaceOffset-offset);
            memcpy(page+offset,composite,ckLen);
            *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned)) += ckLen; //Change free space indicator
            //newChildEntry.valid = false;
        }else{
        	//if(rid.pageNum % 50 == 0)
        	cout<<"Current rid:"<<rid.pageNum<<" "<<rid.slotNum<<endl;
            rc = splitDataEntry(ixFileHandle,newChildEntry,attribute,offset,page,composite,ckLen,pageNumber);
            //cout<<"split data entry return "<<rc<<endl;
            if(rc != 0)
                return -1;
        }
    }else{
        //Non-leaf node case
        //Search for the right index entry in order to enter the next level of B+ tree
        unsigned offset;
        rc = searchEntry(ixFileHandle,attribute,indexEntry(keyEntry),page,offset);
        if(rc != 0)
            return -1;
        
        cout<<"Page number for the next level:"<<*(unsigned *)(page+offset-sizeof(unsigned))<<endl;
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
            rc = searchEntry(ixFileHandle,attribute,newChildEntry,page,offset);
            if(rc != 0)
                return -1;

            if(!isFull){ //enough space,just insert
                //Insert
                if(offset < freeSpaceOffset)
                    memmove(page+offset+iLen,page+offset,freeSpaceOffset-offset);
                memcpy(page+offset,bin,iLen);
                *(unsigned *)(page+PAGE_SIZE-sizeof(unsigned)) += iLen; //Change free space indicator
                newChildEntry.valid = false; //Set newChildEntry to be NULL
            }else{ //Since no enough space,split index node
            	cout<<"Current index page number:"<<pageNumber<<endl;
                rc = splitIndexEntry(ixFileHandle,newChildEntry,attribute,offset,page,bin,iLen,pageNumber);
                //cout<<"Split index page return "<<rc<<endl;
                if(rc != 0)
                    return -1;
                //If this page is root node,then create a new root node
                //cout<<"Root page number:"<<ixFileHandle.rootPage<<endl;
                if(pageNumber == ixFileHandle.rootPage){
                    unsigned curRoot;
                    //cout<<"Creating new root..."<<endl;
                    rc = createNewRoot(ixFileHandle,newChildEntry,attribute,pageNumber,curRoot);
                    //cout<<"New root page number:"<<curRoot<<endl;
                    if(rc != 0)
                        return -1;
                    ixFileHandle.rootPage = curRoot;
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

	indexEntry newChildEntry;
	int rc = backtraceInsert(ixFileHandle, ixFileHandle.rootPage, attribute, key, rid, newChildEntry);
	if(rc != 0)
	    return -1;

    return 0;
}

RC IX_ScanIterator::findFirstLeafPage(IXFileHandle& fileHandle, unsigned& pageNo) {
    if(fileHandle.noPages == 0) {
        return -1;
    }

    byte page[PAGE_SIZE];
    for(unsigned currPage = fileHandle.rootPage ; true ; currPage = *reinterpret_cast<unsigned*>(page)) {
        int rc = fileHandle.readPage(currPage,page);
        if(rc != 0) {
            return -1;
        }

        if(*reinterpret_cast<unsigned*>(page+PAGE_SIZE-2*sizeof(unsigned)) == 1) {
            pageNo = currPage;
            lastReadFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));
            lastReadDataEntryLength = 0;
            return 0;
        }
    }
}

RC IndexManager::searchIndexTree(IXFileHandle& fileHandle, const unsigned pageNumber,
		const Attribute& attribute,const bool lowKeyInclusive, const dataEntry& dataEnt, unsigned& leafPageNo) {
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
    /*
     * searchEntry already considers the condition where key is smaller than the first index key.
     * In this function, resolveNewChildEntry is called after moving page pointer right by a sizeof(unsigned) bytes,
     * to skip the first index pointer.
    */
    unsigned offset;
    rc = searchEntry(fileHandle, attribute, indexEntry(dataEnt),lowKeyInclusive, page, offset);
    if(rc != 0) {
        return -1;
    }

    /*
     * searchEntry() is overloaded and now we have another two overloaded searchEntry()s
     * which are added with a lowKeyInclusive parameter. The reason for adding this is that
     * when lowKeyInclusive = false, we need to skip all duplicates equaling the value of lowkey.
     * To realize this, rid of index entry is no longer compared while searching
     * so that our search process always ends at an index entry with bigger key value.
     * */
    return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, lowKeyInclusive, dataEnt, leafPageNo);

    /*
    if(offset == *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned))) {
        return searchIndexTree(fileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, leafPageNo);
    }

    unsigned iLen;
    indexEntry nextEnt;
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
    }*/
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    dataEntry dataEnt;
    unsigned dummyLength; //not needed
    if(transformKeyRIDPair(attribute, dataEnt, key, rid, dummyLength) != 0) {
        cout << "RC != 0 from transformKeyRIDPair\n";
        return -1;
    }
    cout << "Successful return from transformKeyRIDPair\n";

    if(deleteEntryHelper(ixFileHandle, ixFileHandle.rootPage, attribute, dataEnt, true) != -1) {
        cout << "Successful return from deleteEntryHelper\n";
        return 0;
    }

    cout << "RC == -1 from deleteEntryHelper\n";
    return -1;
}

RC IndexManager::deleteEntryHelper(IXFileHandle &ixFileHandle, const unsigned pageNumber, const Attribute &attribute, const dataEntry& dataEnt, bool amIRoot) {
    char page[PAGE_SIZE];
    RC rc = ixFileHandle.readPage(pageNumber,page);
    if(rc != 0) {
        cout <<"\tdeleteEntryHelper: 1) Page wasn't read succesffuly\n";
        return -1;
    }
    const unsigned freeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));
    unsigned offset;

    //If page is a leaf, delete the data entry, if found
    //Return codes:
    //key not found/internal error: -1
    //key found and deleted without freeing the page: 0
    //key found and deleted at the same time freeing the page: -2
    if(*reinterpret_cast<unsigned*>(page+PAGE_SIZE-2*sizeof(unsigned)) == 1) {
        rc = IndexManager::instance().searchEntry(ixFileHandle, attribute, dataEnt, page, offset);
        if(rc != 0 || offset >= freeSpaceOffset) {  //key not found
            cout <<"\tdeleteEntryHelper: 2) Key not found, returning -1\n";
            return -1;
        }

        dataEntry readDataEntry;
        unsigned readDataEntryLength;
        IndexManager::instance().resolveCompositeKey(page+offset, attribute, readDataEntry, readDataEntryLength);

        if(dataEnt != readDataEntry) {
            cout <<"\tdeleteEntryHelper: Some kind of internal error\n";
            return -1;  //not even sure if it's possible, but..
        }
        else if(freeSpaceOffset == readDataEntryLength) { //found data entry is the only one on page, we have to free the page:
            //we have to update freeSpaceOffset anyway, for the sake of getNextEntry method
            *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned)) -= readDataEntryLength;
            int rightSiblingPageNo = *reinterpret_cast<int*>(page+PAGE_SIZE-3*sizeof(int));
            int leftSiblingPageNo = *reinterpret_cast<int*>(page+PAGE_SIZE-4*sizeof(int));
            rc = ixFileHandle.writePage(pageNumber,page);
            if(rc != 0) {
                cout <<"\tdeleteEntryHelper: 3) Page wasn't written succesffuly\n";
                return -1;
            }

            if(leftSiblingPageNo != -1) {
                rc = ixFileHandle.readPage(leftSiblingPageNo,page);
                if(rc != 0) {
                    cout <<"\tdeleteEntryHelper: 4) Page wasn't read succesffuly\n";
                    return -1;
                }
                *reinterpret_cast<int*>(page+PAGE_SIZE-3*sizeof(int)) = rightSiblingPageNo;
                rc = ixFileHandle.writePage(leftSiblingPageNo,page);
                if(rc != 0) {
                    cout <<"\tdeleteEntryHelper: 5) Page wasn't written succesffuly\n";
                    return -1;
                }
            }
            //TODO: ADD THE PAGE TO LIST OF FREE PAGES ON THE HIDDEN PAGE
            cout <<"\tdeleteEntryHelper: 6) Returning -2\n";
            return -2;
        }
        else {  //leaf page contains more than one data entry
            memmove(page+offset, page+offset+readDataEntryLength, freeSpaceOffset-(offset+readDataEntryLength));
            *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned)) -= readDataEntryLength;
            rc = ixFileHandle.writePage(pageNumber,page);
            if(rc != 0) {
                cout <<"\tdeleteEntryHelper: 7) Page wasn't written succesffuly\n";
                return -1;
            }
            cout <<"\tdeleteEntryHelper: 8) Returning 0\n";
            return 0;
        }
    }
    //Non-leaf page - keep searching for leaf page and then handle situation accordingly
    else {
        indexEntry indexEnt(dataEnt);
        rc = searchEntry(ixFileHandle, attribute, indexEnt, page, offset);
        if(rc != 0) {
            cout <<"\tdeleteEntryHelper: Rather impossible if statement\n";
            return -1;
        }

        rc = deleteEntryHelper(ixFileHandle, *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)), attribute, dataEnt, false);
        if(rc == 0 || rc == -1) { //no deletion took place below this node/page below this node had more than one entry when the deletion took place (rc==0) OR key not found (rc==-1)
            cout <<"\tdeleteEntryHelper: 9) Returning rc = " << rc << " from non-leaf page level\n";
            return rc;
        }
        else if(rc == -2) { //key was deleted from leaf page below this level and the leaf page became empty
            cout <<"\tdeleteEntryHelper: 10) Having read -2 on non-leaf page level..\n";
            unsigned lengthOfIndexEnt = indexEnt.actualLength(attribute);
            //if the length of the index entry + unpaired page pointer == freeSpaceOffset, this page contains only one index entry
            if(lengthOfIndexEnt + sizeof(unsigned) == freeSpaceOffset) {
                unsigned otherChildPageNo;
                if(offset == freeSpaceOffset) {
                    otherChildPageNo = *reinterpret_cast<unsigned*>(page);
                }
                else {
                    otherChildPageNo = *reinterpret_cast<unsigned*>(freeSpaceOffset-sizeof(unsigned));
                }

                if(amIRoot) {
                    //If this parent (non-leaf) node is a root node, then just mark this page as free as well and let the root pointer point to
                    //the other of this marked-for-deletion page's child. Thus, only one leaf page remains and it happens to be the new root.
                    ixFileHandle.rootPage = otherChildPageNo;
                    //TODO: ADD THE PAGE TO LIST OF FREE PAGES ON THE HIDDEN PAGE
                    cout <<"\tdeleteEntryHelper: 11) Returning 0 (root case)\n";
                    return 0;
                }
                else {
                    //If this parent (non-leaf) node is not a root node, then it is also marked for deletion
                    //and it returns a page number of its other child.
                    //TODO: ADD THE PAGE TO LIST OF FREE PAGES ON THE HIDDEN PAGE
                    cout <<"\tdeleteEntryHelper: 12) Returning otherChildPageNo= " << otherChildPageNo <<" (non-root case)\n";
                    return otherChildPageNo;
                }
            }
            else { //not last index entry: shift entries, update freeSpaceOffset and return 0
                                                      //lengthOfIndexEnt already includes sizeof(unsigned) so we have to subtract it
                memmove(page+offset-sizeof(unsigned), page+offset-sizeof(unsigned)+lengthOfIndexEnt, freeSpaceOffset-(offset-sizeof(unsigned)+lengthOfIndexEnt));
                *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned)) -= lengthOfIndexEnt;
                rc = ixFileHandle.writePage(pageNumber,page);
                if(rc != 0) {
                    cout <<"\tdeleteEntryHelper: 13) Page wasn't written succesffuly\n";
                    return -1;
                }
                cout <<"\tdeleteEntryHelper: 14) Returning 0 - not last index entry case\n";
                return 0;
            }
        }
        else { //non-leaf page below this level contained only one index entry, which was deleted,
               //and that page "returned" one of its child's page number that will replace the previous page number in THIS node
            *reinterpret_cast<unsigned*>(page+offset-sizeof(unsigned)) = rc;
            rc = ixFileHandle.writePage(pageNumber,page);
            if(rc != 0) {
                cout <<"\tdeleteEntryHelper: 15) Page wasn't written succesffuly\n";
                return -1;
            }
            cout <<"\tdeleteEntryHelper: 16) Returning 0 - having read page number to replace the current one\n";
            return 0;
        }
    }
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if(!ixFileHandle.isValid()) {
        return -1;
    }
    ix_ScanIterator.setIxFileHandle(&ixFileHandle);
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

    ix_ScanIterator.setScanning(false);
    ix_ScanIterator.setIsNewPage(true); //although it doesn't practically matter at this point..

    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
	//cerr<<"printBtree..."<<" from "<<ixFileHandle.rootPage<<endl;
	printNode(ixFileHandle,attribute,ixFileHandle.rootPage,0);
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
		//cerr<<"Print in leaf page "<<pageNumber<<endl;
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
			cur += cLen;
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
		//cerr<<"Print in index page "<<pageNumber<<endl;
		vector<unsigned> pointers;

		pointers.push_back(*(unsigned *)cur);
		cur += sizeof(unsigned);
		while(cur-node < freeSpaceOffset){
			indexEntry iEntry;
			unsigned iLen;
			resolveNewChildEntry(cur, iEntry, attribute, iLen);
			pointers.push_back(iEntry.pageNum);
			if(attribute.type == AttrType::TypeInt){
				cout<<"\""<<iEntry.ival<<"\"";
			}
			else if(attribute.type == AttrType::TypeReal){
				cout<<"\""<<iEntry.fval<<"\"";
			}else{
				cout<<"\""<<iEntry.key<<"\"";
			}
			//cerr<<"Print key with length of "<<iLen<<endl;
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
		cout<<endl;
		printTab(level);
		cout<<"]}";
	}
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

/*
 * This function determines the start place for scanning.
 * Specifically,IX_ScanIterator::currPage and IX_ScanIterator::currOffset is initialized.
 * In addition, IX_ScanIterator::lastReadFreeSpaceOffset and IX_ScanIterator::lastReadDataEntryLength is initialized.
 * */
RC IX_ScanIterator::determineInitialPageAndOffset() {
    RC rc;
    if(lowKeyInfinity) {
        rc = findFirstLeafPage(*ixFileHandle, currPage);
        if(rc != 0) {
            return rc;
        }
        currOffset = 0;
        return 0;
    }
    else {
    	/*
    	 * readRootPointer() is moved to here and the originally overload function searchIndexTree()
    	 * (the one without the second page number parameter) is deleted for simplification.
    	 */
        rc = IndexManager::instance().searchIndexTree(*ixFileHandle,ixFileHandle->rootPage,attribute,lowKeyInclusive,lowKeyEntry, currPage);
        cout<<"Page number of data entry page to begin scan:"<<currPage<<endl;
        if(rc != 0) {
            return rc;
        }
    }

    char page[PAGE_SIZE];
    rc = ixFileHandle->readPage(currPage,page);
    if(rc != 0) {
        return rc;
    }
    rc = IndexManager::instance().searchEntry(*ixFileHandle, attribute, lowKeyEntry, lowKeyInclusive, page, currOffset);
    cout<<"Include low key?"<<lowKeyInclusive<<" start from(offset):"<<currOffset<<endl;
    if(rc != 0) {
        return rc;
    }
    //If the record is not on the page where it's supposed to be,
    //getNextEntry() will set "currOffset" and "currPage" to the beginning of next page, if any
    lastReadFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));
    lastReadDataEntryLength = 0;
    return 0;
    /*if(currOffset == *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned))) {
        return 0;
    }

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
        if(lowKeyEntry.fval < readDataEntry.fval || (lowKeyEntry.fval == readDataEntry.fval && lowKeyInclusive))
            return 0;
        else if(lowKeyEntry.fval == readDataEntry.fval && !lowKeyInclusive){
            currOffset += readDataEntryLen;
            return 0;
        }
        else
            return -1;
    }
    else {
        if(lowKeyEntry.key < readDataEntry.key || (lowKeyEntry.key == readDataEntry.key && lowKeyInclusive))
            return 0;
        else if(lowKeyEntry.key == readDataEntry.key && !lowKeyInclusive){
            currOffset += readDataEntryLen;
            return 0;
        }
        else
            return -1;
    }*/
}

void IX_ScanIterator::transformDataEntryKey(dataEntry dataEnt, void* key) const {
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
    /*
     * During the very first scan operation we need to find the corresponding page and offset
     * of the first qualifying data entry.
     * If scanStarted is used,it should be set to false
     * every time this function retrieves the last data entry that satisfies the condition.
     */
    if(!scanning) {
        if(determineInitialPageAndOffset() != 0) {
            return IX_EOF;
        }
        scanning = true;
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
    RC rc = ixFileHandle->readPage(currPage,page);
    if(rc != 0) {
        return rc;
    }
    unsigned currentFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));
    /*
     * If deletion happens since last time this function is called,
     * then currOffset should be updated immediately,
     * otherwise currOffset could be greater than currentFreeSpaceOffset even actually it's not,
     * in which case this function will wrongly search for the next page.
     * By the way,it seems that isNewPage(name changed from enteredNewPage) is useless.
     * */
    if(currentFreeSpaceOffset < lastReadFreeSpaceOffset) {
		currOffset -= lastReadDataEntryLength;
		lastReadFreeSpaceOffset = currentFreeSpaceOffset;
	}

    //If it's time to skip to the next page, if any
    // (it's a loop because theoretically there might be an empty page in the middle)
    while(currOffset >= currentFreeSpaceOffset) {
        int nextPage = *reinterpret_cast<int *>(page+PAGE_SIZE-3*sizeof(unsigned));
        //cout<<"Next data entry page:"<<nextPage<<endl;
        if(nextPage == -1) {
        	scanning = false;
            return IX_EOF;
        }

        //There is next page
        rc = ixFileHandle->readPage(nextPage,page);
        if(rc != 0) {
            return rc;
        }
        currPage = nextPage;
        currOffset = 0;
        currentFreeSpaceOffset = *reinterpret_cast<unsigned *>(page+PAGE_SIZE-sizeof(unsigned));
        //isNewPage = true;
        lastReadFreeSpaceOffset = currentFreeSpaceOffset;
    }

    /*
     * If lastReadFreeSpaceOffset and lastReadDataEntryLength is not initialized,
     * there could be undefined results.
     */
    /*if(!isNewPage) {
        if(currentFreeSpaceOffset < lastReadFreeSpaceOffset) {
            currOffset -= lastReadDataEntryLength;
        }
    }
    else {
    	isNewPage = false;
        lastReadFreeSpaceOffset = currentFreeSpaceOffset;
    }*/

    dataEntry readDataEntry;
    IndexManager::instance().resolveCompositeKey(page+currOffset, attribute, readDataEntry, lastReadDataEntryLength);
    if(attribute.type == AttrType::TypeInt) {
        if(!highKeyInfinity && ((highKeyEntry.ival < readDataEntry.ival) || (highKeyEntry.ival == readDataEntry.ival && !highKeyInclusive))) {
        	scanning = false;
        	return IX_EOF;
        }
    }
    else if(attribute.type == AttrType::TypeReal) {
        if(!highKeyInfinity && ((highKeyEntry.fval < readDataEntry.fval) || (highKeyEntry.fval == readDataEntry.fval && !highKeyInclusive))) {
        	scanning = false;
        	return IX_EOF;
        }
    }
    else {
        if(!highKeyInfinity && ((highKeyEntry.key < readDataEntry.key) || (highKeyEntry.key == readDataEntry.key && !highKeyInclusive))) {
        	scanning = false;
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
    //Commented due to the clash with ixtest_03 - author of the test
    //tries to close ixFileHandle in addition to calling this method
    //return PagedFileManager::instance().closeFile(*ixFileHandle);
    return 0; //Who designed this twisted API!?
}

const IXFileHandle* IX_ScanIterator::getIxFileHandle() const {
    return ixFileHandle;
}

void IX_ScanIterator::setIxFileHandle(IXFileHandle* ixFileHandle) {
    this->ixFileHandle = ixFileHandle;
}

const Attribute &IX_ScanIterator::getAttribute() const {
    return attribute;
}

void IX_ScanIterator::setAttribute(const Attribute attribute) {
	this->attribute = attribute;
	//this->attribute.name = attribute.name;
	//this->attribute.type = attribute.type;
	//this->attribute.length = attribute.length;
}

bool IX_ScanIterator::isLowKeyInclusive() const {
    return lowKeyInclusive;
}

void IX_ScanIterator::setLowKeyInclusive(bool lowKeyInclusive) {
	this->lowKeyInclusive = lowKeyInclusive;
}

bool IX_ScanIterator::isHighKeyInclusive() const {
    return highKeyInclusive;
}

void IX_ScanIterator::setHighKeyInclusive(bool highKeyInclusive) {
	this->highKeyInclusive = highKeyInclusive;
}

const dataEntry &IX_ScanIterator::getLowKeyEntry() const {
    return lowKeyEntry;
}

void IX_ScanIterator::setLowKeyEntry(const dataEntry &lowKeyEntry) {
	this->lowKeyEntry = lowKeyEntry;
}

const dataEntry &IX_ScanIterator::getHighKeyEntry() const {
    return highKeyEntry;
}

void IX_ScanIterator::setHighKeyEntry(const dataEntry &highKeyEntry) {
	this->highKeyEntry = highKeyEntry;
}

bool IX_ScanIterator::isLowKeyInfinity() const {
    return lowKeyInfinity;
}

void IX_ScanIterator::setLowKeyInfinity(bool lowKeyInfinity) {
	this->lowKeyInfinity = lowKeyInfinity;
}

bool IX_ScanIterator::isHighKeyInfinity() const {
    return highKeyInfinity;
}

void IX_ScanIterator::setHighKeyInfinity(bool highKeyInfinity) {
	this->highKeyInfinity = highKeyInfinity;
}

bool IX_ScanIterator::isScanning() const {
    return scanning;
}

void IX_ScanIterator::setScanning(bool scanning) {
	this->scanning = scanning;
}

bool IX_ScanIterator::isIsNewPage() const {
    return isNewPage;
}

void IX_ScanIterator::setIsNewPage(bool isNewPage) {
	this->isNewPage = isNewPage;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    noPages = 0;
    lastTableID = 0;
    rootPage = 0;
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

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

