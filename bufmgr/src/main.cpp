#include <iostream>
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"

#define PRINT_ERROR(str) \
{ \
	std::cerr << "On Line No:" << __LINE__ << "\n"; \
	std::cerr << str << "\n"; \
	exit(1); \
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, i;
RecordId rid[num], rid2, rid3;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr* bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test_unPinPage();
void testBufMgr();

int main()
{
	//Following code shows how to you File and Page classes

  const std::string& filename = "test.db";
  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(filename);
  }
	catch(FileNotFoundException)
	{
  }

  {
    // Create a new database file.
    File new_file = File::create(filename);

    // Allocate some pages and put data on them.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i) {
      Page new_page = new_file.allocatePage();
      if (i == 3) {
        // Keep track of the identifier for the third page so we can read it
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      // Write the page back to the file (with the new data).
      new_file.writePage(new_page);
    }

    // Iterate through all pages in the file.
    for (FileIterator iter = new_file.begin();
         iter != new_file.end();
         ++iter) {
      // Iterate through all records on the page.
      for (PageIterator page_iter = (*iter).begin();
           page_iter != (*iter).end();
           ++page_iter) {
        std::cout << "Found record: " << *page_iter
            << " on page " << (*iter).page_number() << "\n";
      }
    }

    // Retrieve the third page and add another record to it.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId& rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    // Retrieve the record we just added to the third page.
    std::cout << "Third page has a new record: "
        << third_page.getRecord(rid) << "\n\n";
  }
  // new_file goes out of scope here, so file is automatically closed.

  // Delete the file since we're done with it.
  File::remove(filename);

	//This function tests buffer manager, comment this line if you don't wish to test buffer manager
	testBufMgr();
}

void testBufMgr()
{
	// create buffer manager
	bufMgr = new BufMgr(num);

	// create dummy files
  const std::string& filename1 = "test.1";
  const std::string& filename2 = "test.2";
  const std::string& filename3 = "test.3";
  const std::string& filename4 = "test.4";
  const std::string& filename5 = "test.5";

  try
	{
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
  }
	catch(FileNotFoundException e)
	{
  }

	File file1 = File::create(filename1);
	File file2 = File::create(filename2);
	File file3 = File::create(filename3);
	File file4 = File::create(filename4);
	File file5 = File::create(filename5);

	file1ptr = &file1;
	file2ptr = &file2;
	file3ptr = &file3;
	file4ptr = &file4;
	file5ptr = &file5;

	//Test buffer manager
	//Comment tests which you do not wish to run now. Tests are dependent on their preceding tests. So, they have to be run in the following order.
	//Commenting  a particular test requires commenting all tests that follow it else those tests would fail.
	test1();
	test2();
	test3();
	test4();
	test5();
	test6();
    test7();
    test_unPinPage();

	//Close files before deleting them
   // printf("~file\n");
	file1.~File();
   // printf("~file1\n");
	file2.~File();
	file3.~File();
	file4.~File();
	file5.~File();
   // printf("starting remove\n");
	//Delete files
	File::remove(filename1);
	File::remove(filename2);
	File::remove(filename3);
	File::remove(filename4);
	File::remove(filename5);
    printf("starting delete bufmgr\n");
	delete bufMgr;

	std::cout << "\n" << "Passed all tests." << "\n";
}

void test1()
{
	//Allocating pages in a file...
	for (i = 0; i < num; i++)
	{
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
       // printf("index %i \n", i);
	}

	//Reading pages back...
	for (i = 0; i < num; i++)
	{
        //printf("start reading page, index %i\n", i);
		bufMgr->readPage(file1ptr, pid[i], page);
        //printf("readPage return\n");
		sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
        //printf("passed error msg\n");
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	std::cout<< "Test 1 passed" << "\n";
}

void test2()
{
	//Writing and reading back multiple files
	//The page number and the value should match

	for (i = 0; i < num/3; i++)
	{
        //printf("in test 2, waiting for allocPage %i times\n", i);
		bufMgr->allocPage(file2ptr, pageno2, page2);
		sprintf((char*)tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		rid2 = page2->insertRecord(tmpbuf);
        //printf("passed thru allocPage %i times\n", i);
		int index = random() % num;
    pageno1 = pid[index];
		bufMgr->readPage(file1ptr, pageno1, page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pageno1, (float)pageno1);
		if(strncmp(page->getRecord(rid[index]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->allocPage(file3ptr, pageno3, page3);
		sprintf((char*)tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		rid3 = page3->insertRecord(tmpbuf);

		bufMgr->readPage(file2ptr, pageno2, page2);
		sprintf((char*)&tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		if(strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->readPage(file3ptr, pageno3, page3);
		sprintf((char*)&tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		if(strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->unPinPage(file1ptr, pageno1, false);
	}

	for (i = 0; i < num/3; i++) {
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
	}

	std::cout << "Test 2 passed" << "\n";
}

void test3()
{
	try
	{
		bufMgr->readPage(file4ptr, 1, page);
		PRINT_ERROR("ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
	}
	catch(InvalidPageException e)
	{
	}

	std::cout << "Test 3 passed" << "\n";
}

void test4()
{
	bufMgr->allocPage(file4ptr, i, page);
	bufMgr->unPinPage(file4ptr, i, true);
	try
	{
        //printf("unpin try pageId: %i\n", i);
		bufMgr->unPinPage(file4ptr, i, false);
		PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
	}
	catch(PageNotPinnedException e)
	{
        printf("catch the exception in test 4, hoorrray\n");
	}

	std::cout << "Test 4 passed" << "\n";
}

void test5()
{
	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file5ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.5 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
	}

	PageId tmp;
	try
	{
		bufMgr->allocPage(file5ptr, tmp, page);
		PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
	}
	catch(BufferExceededException e)
	{
	}

	std::cout << "Test 5 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
	//flushing file with pages still pinned. Should generate an error
	for (i = 1; i <= num; i++) {
		bufMgr->readPage(file1ptr, i, page);
	}

	try
	{
		bufMgr->flushFile(file1ptr);
		PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
	}
	catch(PagePinnedException e)
	{
	}

	std::cout << "Test 6 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file1ptr, i, true);

	bufMgr->flushFile(file1ptr);
}

void test7()
{
    bufMgr->allocPage(file1ptr, pageno1, page);
    // disposePage should throw PagePinnedException if page is still pinned.
    try
    {
        bufMgr->disposePage(file1ptr, pageno1);
        PRINT_ERROR("ERROR :: Pages pinned for file being disposed. Exception should have been thrown before execution reaches this point.");
    }
    catch(PagePinnedException& e)
    {
        // Do nothing
        printf("in page pinned\n");
    }

    // disposePage should not throw exceptions even though no longer in buffer
    bufMgr->unPinPage(file1ptr, pageno1, false);
    for (i = 0; i < num; ++i)
    {
        bufMgr->allocPage(file2ptr, pid[i], page);      // Evict file1's frame
    }
    try
    {
        bufMgr->disposePage(file1ptr, pageno1);
    }
    catch(PageNotPinnedException& e)
    {
        PRINT_ERROR("ERROR :: Page is unpinned. disposePage should not throw any exceptions.");
    }
    // Second disposePage should throw InvalidPageException.
    try
    {
        bufMgr->disposePage(file1ptr, pageno1);
        PRINT_ERROR("ERROR :: disposePage should throw InvalidPageException when page is already disposed.");
    }
    catch(InvalidPageException& e)
    {
        printf("InvalidPageException here. No worries, catching it here=P\n");
    }
    for(i = 0; i < num; ++i)
    {
        bufMgr->unPinPage(file2ptr, pid[i], false);
    }

    std::cout << "Test 7 passed" << "\n";
}

void test_unPinPage() {
    // Test conditions:  File 1 exists, and has at least one page.
    // After test conditions: File 1 will be flushed from buffer.

    PageId firstPageID = 1;
    const bool notDirty = 0;
    const bool isDirty = 1;
    Page *testPage;

    // Flush all file 1 to get rid of test page
    bufMgr->flushFile(file1ptr);

    // Test nothing happens if page not in buffer
    try {
        bufMgr->unPinPage(file1ptr, firstPageID, notDirty);
    } catch (...) {
        PRINT_ERROR("ERROR :: Page is not in buffer. unPinPage should not throw any exceptions.");
    }

    // Test unPinPage on unpinned page results in PageNotPinned exception
    //      also implicitly tests successful unPinPage if readPage is working
    bufMgr->readPage(file1ptr, firstPageID, testPage);
    bufMgr->unPinPage(file1ptr, firstPageID, notDirty);
    try {
        bufMgr->unPinPage(file1ptr, firstPageID, notDirty);
        PRINT_ERROR("ERROR :: Page is already unpinned. unPinPage should PageNotPinnedException.");
    } catch (PageNotPinnedException& e) {
        // Do nothing
    }

    // Test unPinPage sets dirty bit to true
    bufMgr->readPage(file1ptr, firstPageID, testPage);
    sprintf((char*)tmpbuf, "Testing unPinPage");
    const RecordId& rid = testPage->insertRecord(tmpbuf);
    bufMgr->unPinPage(file1ptr, firstPageID, isDirty);
    bufMgr->flushFile(file1ptr);

    bufMgr->readPage(file1ptr, firstPageID, testPage);
    if(strncmp(testPage->getRecord(rid).c_str(), tmpbuf , strlen(tmpbuf)) != 0)
    {
        PRINT_ERROR("ERROR :: UnPinPage did not set dirty bit correctly");
    }
    testPage->deleteRecord(rid);
    bufMgr->unPinPage(file1ptr, firstPageID, notDirty);

    // Test unPinPage does not set dirty bit to false
    bufMgr->readPage(file1ptr, firstPageID, testPage);
    sprintf((char*)tmpbuf, "Testing unPinPage");
    const RecordId& rid2 = testPage->insertRecord(tmpbuf); // rid2 == rid since we're the only users?
    bufMgr->unPinPage(file1ptr, firstPageID, isDirty);
    bufMgr->readPage(file1ptr, firstPageID, testPage);
    bufMgr->unPinPage(file1ptr, firstPageID, notDirty);
    bufMgr->flushFile(file1ptr);

    bufMgr->readPage(file1ptr, firstPageID, testPage);
    if(strncmp(testPage->getRecord(rid2).c_str(), tmpbuf , strlen(tmpbuf)) != 0)
    {
        PRINT_ERROR("ERROR :: UnPinPage did not set dirty bit correctly");
    }
    testPage->deleteRecord(rid2);
    bufMgr->unPinPage(file1ptr, firstPageID, notDirty);

    // Flush file to attain guaranteed post test state.
    bufMgr->flushFile(file1ptr);

    std::cout << "Test for unPinPage passed\n";
}
