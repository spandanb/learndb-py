import logging
import os.path
import sys

from .constants import (
    TABLE_MAX_PAGES,
    PAGE_SIZE,
    EXIT_FAILURE,
    FILE_HEADER_OFFSET,
    FILE_HEADER_SIZE,
    FILE_PAGE_AREA_OFFSET,
    FILE_HEADER_VERSION_FIELD_SIZE,
    FILE_HEADER_VERSION_FIELD_OFFSET,
    FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE,
    FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET,
    FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET,
    FREE_PAGE_NEXT_FREE_PAGE_HEAD_SIZE,
    FILE_HEADER_VERSION_VALUE,
    NULLPTR,
)


class InvalidPageAccess(Exception):
    pass


class Pager:
    """
    Manages pages in memory (cache) and on file.

    The pager provides page abstraction on top of the file's byte stream.
    From the pager's perspective, the pager sees the file organized like:
    file_header, page_0, page_1, ... page_N-1.

    Page allocation is thus:
        - when pages are returned, they are kept in a list (in-memory)
        - when the db is shutdown, the free pages are persisted on disk
            via a singly linked list. The head of this list is stored in the
            file header. Each free page contains the page num of the next free
            page.
        - if a new page is requested, it should be sourced
          in the following order:
            - in memory list of free pages
            - on disk list of free pages
            - end of file (by increasing file by a page size)
    """
    def __init__(self, filename: str):
        self.header = None
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]
        self.filename = filename
        self.fileptr = None
        self.file_length = 0
        # num of actual pages
        # at startup, this equals the number of pages on disk; once the pager is running
        # it's the number of pages in memory
        self.num_pages = 0
        # number of pages on disk
        self.num_pages_on_disk = 0
        # the next free page num to alloc - should monotonically increase
        self.next_allocatable_page_num = 0
        self.returned_pages = []
        # linked-list of free pages
        self.free_page_list_head = NULLPTR
        self.init()

    @classmethod
    def pager_open(cls, filename):
        """
        Create pager on argument file
        """
        return cls(filename)

    def get_unused_page_num(self) -> int:
        """
        NOTE: this depends on num_pages being updated when a new page is requested
        # todo: rename get_free_page_num
        :return:
        """
        # first check the on-memory page cache
        if len(self.returned_pages):
            return self.returned_pages.pop()

        # check the on-disk free list
        if self.free_page_list_head != NULLPTR:
            page_num = self.free_page_list_head
            page = self.get_page(page_num)
            next_page_num = self.get_free_page_next(page)
            # set current next as free list head
            self.free_page_list_head = next_page_num
            return page_num

        # allocate at end of file
        free_page_num = self.next_allocatable_page_num
        # once allocated, incr page num to avoid double allocation
        self.next_allocatable_page_num += 1
        return free_page_num

    def page_exists(self, page_num: int) -> bool:
        """

        :param page_num: does this page exist/ has been allocated
        :return:
        """
        # num_pages counts whole pages
        return page_num < self.num_pages

    def get_page(self, page_num: int) -> bytearray:
        """
        get `page` given `page_num`
        """
        if page_num >= TABLE_MAX_PAGES:
            raise InvalidPageAccess(f"Tried to fetch page out of bounds (requested page = {page_num}, max pages = {TABLE_MAX_PAGES})")

        if self.pages[page_num] is None:
            # cache miss. Allocate memory and load from file.
            page = bytearray(PAGE_SIZE)

            # determine number of pages in file; there should only be complete pages
            if page_num < self.num_pages:
                # this page exists on file, load from file
                # into `page`
                self.fileptr.seek(FILE_PAGE_AREA_OFFSET + page_num * PAGE_SIZE)
                read_page = self.fileptr.read(PAGE_SIZE)
                assert len(read_page) == PAGE_SIZE, "corrupt file: read page returned byte array smaller than page"
                page[:PAGE_SIZE] = read_page

            self.pages[page_num] = page

            if page_num >= self.num_pages:
                self.num_pages = page_num + 1

            if self.next_allocatable_page_num < self.num_pages:
                # next alloc must be at end of file and monotonically increasing
                self.next_allocatable_page_num = self.num_pages

        return self.pages[page_num]

    def return_page(self, page_num: int):
        """

        :param page_num:
        :return:
        """
        self.returned_pages.append(page_num)

    def truncate_file(self):
        """
        Check if there are any to-be recycled pages in memory at
        tail of the file. If so truncate file and remove page.
        :return:
        """
        if not self.returned_pages or self.file_length == 0:
            """
            no in-memory pages, no-op
            """
            return
        self.returned_pages.sort()
        page_num = self.returned_pages[-1]
        while page_num:
            # check if: 1) is tail page and 2) is on disk
            if page_num == self.num_pages - 1 and page_num == self.num_pages_on_disk - 1:
                # truncate file
                self.file_length -= PAGE_SIZE
                assert self.file_length >= 0, f"invalid file length {self.file_length}"
                self.fileptr.truncate(self.file_length)
                self.num_pages -= 1
                self.num_pages_on_disk -= 1
                self.returned_pages.pop()

                page_num = self.returned_pages[-1] if len(self.returned_pages) > 0 else None
            else:
                break

    def close(self):
        """
        close the pager. flush header and pages to file
        """
        # check if file can be truncated
        self.truncate_file()
        # flush free pages
        # add pages to on-disk free list
        head = self.free_page_list_head
        while self.returned_pages:
            page_num = self.returned_pages.pop()
            page = self.get_page(page_num)
            # set next
            self.set_free_page_next(page, head)
            # flush free pages since they contain the next free page pointer
            self.flush_page(page_num)
            head = page_num

        # update header with free list head
        self.set_free_page_head(self.header, head)
        # flush updated header
        self.flush_header()

        # flush in-use pages
        # pages are 0-based
        for page_num in range(self.num_pages):
            if self.pages[page_num] is None:
                continue
            self.flush_page(page_num)

        # close file
        self.fileptr.close()

    # section: internal API

    def init(self):
        """
        Initialize pager. This includes:
            - open database file
            - read file header and get next free page
            - set state vars like num_pages (in file), file length etc.
            - warm up pager cache, by loading pages into dis
        """
        # open binary file such that: it is readable, not truncated(random),
        # create if not exists, writable(random)
        # a+b (and more generally any "a") mode can only write to end
        # of file; seeks only applies to read ops
        # r+b allows read and write, without truncation, but errors if
        # the file does not exist
        # NB: this sets the file ptr location to the end of the file
        try:
            # file exists
            self.fileptr = open(self.filename, "r+b")
            self.read_file_header()
        except FileNotFoundError:
            # file does not exist
            self.fileptr = open(self.filename, "w+b")
            self.create_file_header()
        self.file_length = os.path.getsize(self.filename)

        if self.file_length % PAGE_SIZE != 0 and (self.file_length - FILE_HEADER_SIZE) % PAGE_SIZE != 0:
            logging.error("Db file is not a valid size. Corrupt file.")
            sys.exit(EXIT_FAILURE)

        self.num_pages = (self.file_length - FILE_HEADER_SIZE) // PAGE_SIZE if self.file_length != 0 else 0
        self.num_pages_on_disk = self.num_pages
        # next free page is the last page of the file
        self.next_allocatable_page_num = self.num_pages

        # warm up page cache, i.e. load pages into memory
        # to load data, seek to beginning of file
        self.fileptr.seek(FILE_HEADER_SIZE)
        for page_num in range(self.num_pages):
            self.get_page(page_num)

    def create_file_header(self, free_page_list_head: int = NULLPTR):
        """
        generate header
        :param free_page_list_head:
        :return:
        """
        header = bytearray(FILE_HEADER_SIZE)
        assert FILE_HEADER_VERSION_FIELD_SIZE >= len(FILE_HEADER_VERSION_VALUE)
        # set version field
        header[FILE_HEADER_VERSION_FIELD_OFFSET:
               FILE_HEADER_VERSION_FIELD_OFFSET + FILE_HEADER_VERSION_FIELD_SIZE] = FILE_HEADER_VERSION_VALUE

        # add free page head
        value = free_page_list_head.to_bytes(FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE, sys.byteorder)
        header[FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET:
                FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET + FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE] = value

        self.header = header

    def read_file_header(self):
        """
        read the file header, formatted like:

        version_string next_free_page padding
        version_string  -> "learndb v<VersionNum>"
        next_free_page -> int, next page_num

        :return:
        """
        # read header
        self.fileptr.seek(0)
        self.header = bytearray(self.fileptr.read(FILE_HEADER_SIZE))
        # get free list head ptr
        next_free_page_bytes = self.header[FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET:
                                    FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET + FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE]
        next_free_page = int.from_bytes(next_free_page_bytes, sys.byteorder)
        if next_free_page != NULLPTR:
            self.free_page_list_head = next_free_page

    @staticmethod
    def get_free_page_next(page: bytes) -> int:
        """
        read next ptr on free page
        :param page:
        :return:
        """
        value = page[FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET:
                        FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET + FREE_PAGE_NEXT_FREE_PAGE_HEAD_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def set_free_page_next(page: bytearray, next_page_num: int):
        """
        set next ptr on page
        :param page:
        :param next_page_num:
        :return:
        """
        value = next_page_num.to_bytes(FREE_PAGE_NEXT_FREE_PAGE_HEAD_SIZE, sys.byteorder)
        page[FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET:
             FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET + FREE_PAGE_NEXT_FREE_PAGE_HEAD_SIZE] = value

    @staticmethod
    def set_free_page_head(header: bytearray, next_page_num: int):
        value = next_page_num.to_bytes(FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE, sys.byteorder)
        header[FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET:
               FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET + FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE] = value

    def flush_header(self):
        """
        Flush file header
        :return:
        """
        byte_offset = FILE_HEADER_OFFSET
        self.fileptr.seek(byte_offset)
        to_write = self.header
        self.fileptr.write(to_write)

    def flush_page(self, page_num: int):
        """
        flush/write page to file
        page_num is the page to write
        size is the number of bytes to write
        """
        if self.pages[page_num] is None:
            logging.error("Tried to flush null page")
            sys.exit(EXIT_FAILURE)

        byte_offset = FILE_PAGE_AREA_OFFSET + page_num * PAGE_SIZE
        self.fileptr.seek(byte_offset)
        to_write = self.pages[page_num]
        self.fileptr.write(to_write)

