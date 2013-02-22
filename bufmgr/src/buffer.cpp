/****************************************************************************
 *   Name:           Kah Jing Lee
 *   UW Campus ID:   906 482 2100
 *   email:          klee224@wisc.edu
 *   timestamp:      Thu Feb 21 17:08:32 CST 2013
 *
 *   File:           buffer.cpp
 *   HW:             2
 ****************************************************************************/

/*
 * Buffer manager serves to maintain the buffer pool.
 * Buffer pool stores pages from disk in the main memory for faster
 * access purpose. Buffer manager has several function related to it
 * which enables the simple read, delete, and allocate pages in the buffer
 * pool.
 */

/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

//#include <cstring>
//#include <string>

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
    bufDescTable = new BufDesc[bufs]; // allocating heap mem. for bufDescTable

    // initialize necessary frames number
    for (FrameId i = 0; i < bufs; i++)
    {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs]; // allocating spaces for page

    // determining the value for hashtable
    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
    clockHand = bufs - 1; // point the the last element
}


BufMgr::~BufMgr() {
    // loop thru every frame and check
    // flush if the frame is valid and dirty
    for(FrameId i = 0; i < numBufs; i++) {
        if(bufDescTable[i].valid == true && bufDescTable[i].dirty == true) {
            flushFile(bufDescTable[i].file);
        }
    }

    // deallocating all dynamically allocated memory
    delete[] bufDescTable;
    delete hashTable;
    delete[] bufPool;
}

void BufMgr::advanceClock()
{
    // advancing clockHand, keep in the numBufs size
    clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame)
{
    unsigned int counter = 0; // counter to check if all pages are pinned

    // implementing clock algorithm
    do {
        // check if the frame is valid, if so, proceed down the clock algo.
        if(bufDescTable[clockHand].valid == true) {
            // reset the refbit if the frame has been referenced recently
            // and advance clock
            if(bufDescTable[clockHand].refbit == true) {
                bufDescTable[clockHand].refbit = false;
                advanceClock();
            }
            // increase counter for # of pinned pages
            else if(bufDescTable[clockHand].pinCnt != 0) {
                counter++;
                advanceClock();
            }
            // if a frame is not pinned and dirty, flush it first before allocating it
            else if(bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty == true) {
                flushFile(bufDescTable[clockHand].file);
                frame = clockHand;
                return;
            }
            // if it is not pinned and not dirty, allocate it straight away
            else if(bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty == false) {
                hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                frame = clockHand;
                return;
            }
        }
        // if frame not valid, can allocate it straight away
        else if(bufDescTable[clockHand].valid == false)
        {
            frame = clockHand;
            return;
        }
    } while(counter <= numBufs);

    // if all pages are pinned, throw BufferExceededException
    throw BufferExceededException();

}

/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNumber;    // used to get frameNumber
    try
    {
        // get frameNumber and set the refbit, and then increase pinCount
        // also, return the ptr to the page
        hashTable->lookup(file, pageNo, frameNumber);
        bufDescTable[frameNumber].refbit = true;
        bufDescTable[frameNumber].pinCnt++;
        page = &bufPool[frameNumber];
    }
    catch(HashNotFoundException e)
    {
        // if the page is not yet existed in the hashtable, allocate it and
        // set the bufDescTable
        // also, return the ptr to the page and its pageNo
        FrameId frameFree;
        allocBuf(frameFree);
        bufPool[frameFree] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frameFree);
        bufDescTable[frameFree].Set(file, pageNo);
        page = &bufPool[frameFree];
    }
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
    FrameId frameNumber;
    try
    {
        // check if the page exist in the frame pool
        hashTable->lookup(file, pageNo, frameNumber);
        // decrement the pincount when unpin, when dirty is set true, set it to the bufDescTable as well
        if(bufDescTable[frameNumber].pinCnt > 0) {
            bufDescTable[frameNumber].pinCnt -= 1;
            if(dirty == true) {
                bufDescTable[frameNumber].dirty = true;
            }
            return;
        }
        // if not pinned, throw exception
        else if(bufDescTable[frameNumber].pinCnt == 0){
            throw PageNotPinnedException(file->filename(), bufDescTable[frameNumber].pageNo, frameNumber);
        }
    }
    catch(HashNotFoundException e)
    {
    }
}

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file   	File object
 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file)
{
    // if the frame corresponding to the file is invalid, throw exception
    // if some1 is referring to the frame, throw exception
    for(unsigned int i = 0; i < numBufs; i++) {
        if(bufDescTable[i].valid == false && bufDescTable[i].file == file) {
            throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
        }
        if(bufDescTable[i].file == file && bufDescTable[i].pinCnt > 0) {
            throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
        }
        // if the frame is valid and dirty, flush it and remove it from hashtable
        // clear the bufDescTable as well
        if(bufDescTable[i].valid == true && bufDescTable[i].file == file) {
            if(bufDescTable[i].dirty == true) {
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            hashTable->remove(bufDescTable[i].file,
                    bufDescTable[i].pageNo);
            bufDescTable[i].Clear();
        }
    }
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
    // allocate new frame by calling allocatePage
    // return the ptr to the page and also value of pageNo
    FrameId frameNumber;
    allocBuf(frameNumber);
    bufPool[frameNumber] = file->allocatePage();
    page = &bufPool[frameNumber];
    pageNo = page->page_number();
    hashTable->insert(file, pageNo, frameNumber);
    bufDescTable[frameNumber].Set(file, pageNo);
}

/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    // dispose the page if it is existed
    // throw error if the page if pinned
    //for(unsigned int i = 0; i < numBufs; i++) {
    //    if(bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo) {
    FrameId frameNo;
    try {
        hashTable->lookup(file, PageNo, frameNo);
        if(bufDescTable[frameNo].pinCnt != 0) {
                throw PagePinnedException(bufDescTable[frameNo].file->filename(), bufDescTable[frameNo].pageNo, bufDescTable[frameNo].frameNo);
        }
            bufDescTable[frameNo].Clear();
            hashTable->remove(file, PageNo);
            file->deletePage(PageNo);
            return;
        }
    catch(HashNotFoundException e) {
        file->deletePage(PageNo);
    }

    // if the page does not exist in the buffer pool, delete it from it file on disk as well
    // unsure what will happen if the page does not exist even on disk
    // opppss, will throw error without deleting anything
}

// printing the usage of buffer pool frame
void BufMgr::printSelf(void)
{
    BufDesc* tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

        if (tmpbuf->valid == true)
            validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
