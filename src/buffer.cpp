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

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    BufMgr::~BufMgr() {
    }

    void BufMgr::advanceClock() {
        // 若小于总 buffer 数自增，大于则归 0
        if (clockHand < numBufs - 1) {
            clockHand++;
        } else {
            clockHand = 0;
        }
    }

    void BufMgr::allocBuf(FrameId &frame) {
        int cnt = 0;
        advanceClock();
        while (true)
        {
            if (!bufDescTable[clockHand].valid) {
                break;
            }
            if (bufDescTable[clockHand].refbit)
            {
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
            if (bufDescTable[clockHand].pinCnt > 0)
            {
                cnt ++;
                if (cnt == numBufs)
                    throw BufferExceededException();
                advanceClock();
                continue;
            }
            if (bufDescTable[clockHand].dirty)
            {
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                bufDescTable[clockHand].dirty = false;
            }
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            break;
        }
        bufDescTable[clockHand].Clear();
        frame = clockHand;
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        // First check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number
        FrameId frameNo;
        try {
            hashTable->lookup(file, pageNo, frameNo);
            bufDescTable[frameNo].refbit = true;
            bufDescTable[frameNo].pinCnt++;
            page = &bufPool[frameNo];
        } catch(HashNotFoundException &){
            allocBuf(frameNo);
            bufPool[frameNo] = file->readPage(pageNo);
            hashTable->insert(file, pageNo, frameNo);
            bufDescTable[frameNo].Set(file, pageNo);
            page = &bufPool[frameNo];
        }
    }


    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        bool found = false;
        for (int i = 0; i < numBufs; i++) {
            if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo) {
                if (bufDescTable[i].pinCnt == 0) {
                    // std::cout << "scan" << i << std::endl;
                    throw PageNotPinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                }
                found = true;
            }
        }
        if (!found) {
            std::cout << "not found" << std::endl;
            return;
        }
        for (int i = 0; i < numBufs; i++) {
            if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo) {
                bufDescTable[i].pinCnt--;
                if (dirty) {
                    bufDescTable[i].dirty = true;
                }
            }
        }
    }

    void BufMgr::flushFile(const File *file) {
        // Throws PagePinnedException if some page of the file is pinned. Throws BadBufferException if an invalid page belonging to the file is encountered.
        for (int i = 0; i < numBufs; i++) {
            if (bufDescTable[i].file == file && bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }
            if (bufDescTable[i].file == file && !bufDescTable[i].valid) {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
        }
        // if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty bit for the page to false
        for (int i = 0; i < numBufs; i++) {
            if (bufDescTable[i].file == file && bufDescTable[i].dirty) {
                // What?
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            // remove the page from the hashtable (whether the page is clean or dirty)
            hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
            // invoke the Clear() method of BufDesc for the page frame
            bufDescTable[i].Clear();
        }
        //scan for pages that belong to the file
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        // The first step in this method is to to allocate an empty page in the specified file by invoking the file->allocatePage() method.
        Page allocatedPage = file->allocatePage();
        pageNo = allocatedPage.page_number();
        // Then allocBuf() is called to obtain a buffer pool frame.
        FrameId frameNo;
        allocBuf(frameNo);
        bufPool[frameNo] = allocatedPage;
        // Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        // return &page
        page = &bufPool[frameNo];
        // Log
        // std::cout << "Allocated Page Number " << pageNo << "." << std::endl;
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {

    }

    void BufMgr::printSelf(void) {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
