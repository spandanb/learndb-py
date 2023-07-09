"""
Get a page, return a page. close pager.
"""
import os

from .context import Pager
from .test_constants import TEST_DB_FILE


def test_free_pages_persisted():
    """
    Test that returned pages are persisted
    and re-served after pager is closed and reopened.
    :return:
    """
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

    pager = Pager(TEST_DB_FILE)
    first = pager.get_unused_page_num()
    second = pager.get_unused_page_num()
    third = pager.get_unused_page_num()

    # don't return third - to avoid file truncation
    pager.return_page(first)
    pager.return_page(second)
    returned_pages = {first, second}

    # close pager and see if the returned pages
    # are given when an unused page is requested
    pager.close()

    pager = Pager(TEST_DB_FILE)
    new_page = pager.get_unused_page_num()
    assert new_page in returned_pages
    new_page = pager.get_unused_page_num()
    assert new_page in returned_pages