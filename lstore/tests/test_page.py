import pytest
from lstore.page import Page
from lstore.config import *

class TestPage:

    # can create a new Page object
    def test_create_page_object(self):
        page = Page()
        assert isinstance(page, Page)

    # can check if there is capacity for a new record
    def test_check_capacity(self):
        page = Page()
        assert page.has_capacity() == True

    # can get the current offset for writing
    def test_get_current_offset(self):
        page = Page()
        assert page.current_offset() == 0

    # can handle writing to a full page
    def test_write_full_page(self):
        page = Page()
        for i in range(page.MAX_RECORDS_PER_PAGE):
            page.write(i)
        assert page.has_capacity() == False

    # can handle reading from an invalid offset
    def test_read_invalid_offset(self):
        page = Page()
        with pytest.raises(IndexError):
            page.read(1)

    # can handle updating an invalid offset
    def test_update_invalid_offset(self):
        page = Page()
        with pytest.raises(IndexError):
            page.update_entry(1, 10)

    # can write a new record to the page
    def test_write_record(self):
        page = Page()
        page.write(10)
        assert page.num_records == 1
        assert page.data[:COLUMN_DATA_SIZE] == b'\n\x00\x00\x00\x00\x00\x00\x00'

    # can read a record from the page
    def test_read_record(self):
        page = Page()
        page.write(10)
        assert page.read(0) == 10

    # can update an existing record on the page
    def test_update_record(self):
        page = Page()
        page.write(10)
        page.update_entry(0, 20)
        assert page.read(0) == 20