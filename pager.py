import os.path
import sys

from constants import TABLE_MAX_PAGES, PAGE_SIZE, EXIT_FAILURE


class InvalidPageAccess(Exception):
    pass


class Pager:
    """
    Manages pages in memory (cache) and on file
    """
    def __init__(self, filename):
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]
        self.filename = filename
        self.fileptr = None
        self.file_length = 0
        self.num_pages = 0
        self.open_file()
        # NOTE: returned pages are lost when the db program is terminated
        # To avoid this, these pages should be kept on an on-disk data structure, e.g.
        # linked-list
        self.returned_pages = []

    def open_file(self):
        """
        open database file
        """
        # open binary file such that: it is readable, not truncated(random),
        # create if not exists, writable(random)
        # a+b (and more generally any "a") mode can only write to end
        # of file; seeks only applies to read ops
        # r+b allows read and write, without truncation, but errors if
        # the file does not exist
        # NB: this sets the file ptr location to the end of the file
        try:
            self.fileptr = open(self.filename, "r+b")
        except FileNotFoundError:
            self.fileptr = open(self.filename, "w+b")
        self.file_length = os.path.getsize(self.filename)

        if self.file_length % PAGE_SIZE != 0:
            # avoiding exceptions since I want this to be closer to Rust, i.e panic or enum
            print("Db file is not a whole number of pages. Corrupt file.")
            sys.exit(EXIT_FAILURE)

        self.num_pages = self.file_length // PAGE_SIZE

        # warm up page cache, i.e. load data into memory
        # to load data, seek to beginning of file
        self.fileptr.seek(0)
        for page_num in range(self.num_pages):
            self.get_page(page_num)

    @classmethod
    def pager_open(cls, filename):
        """
        Create pager on argument file
        """
        return cls(filename)

    def get_unused_page_num(self) -> int:
        """
        NOTE: this depends on num_pages being updated when a new page is requested
        :return:
        """
        if len(self.returned_pages):
            # first check the returned page cache
            return self.returned_pages.pop()
        return self.num_pages

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
            num_pages = self.file_length // PAGE_SIZE
            if page_num < num_pages:
                # this page exists on file, load from file
                # into `page`
                self.fileptr.seek(page_num * PAGE_SIZE)
                read_page = self.fileptr.read(PAGE_SIZE)
                assert len(read_page) == PAGE_SIZE, "corrupt file: read page returned byte array smaller than page"
                page[:PAGE_SIZE] = read_page
            else:
                pass

            self.pages[page_num] = page

            if page_num >= self.num_pages:
                self.num_pages += 1

        return self.pages[page_num]

    def return_page(self, page_num: int):
        """

        :param page_num:
        :return:
        """
        # cleaning it to catch issues with invalid refs
        # self.get_page(page_num)[:PAGE_SIZE] = bytearray(PAGE_SIZE)
        self.returned_pages.append(page_num)

    def close(self):
        """
        close the connection i.e. flush pages to file
        TODO: unused pages should be persisted, e.g. to on-disk linked list.
        """
        # this is 0-based
        # NOTE: not sure about this +1;
        for page_num in range(self.num_pages):
            if self.pages[page_num] is None:
                continue
            self.flush_page(page_num)
        self.fileptr.close()

    def flush_page(self, page_num: int):
        """
        flush/write page to file
        page_num is the page to write
        size is the number of bytes to write
        """
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            sys.exit(EXIT_FAILURE)

        byte_offset = page_num * PAGE_SIZE
        self.fileptr.seek(byte_offset)
        to_write = self.pages[page_num]
        self.fileptr.write(to_write)

