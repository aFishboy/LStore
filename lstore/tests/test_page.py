import pytest
from ..page import Page
from ..config import *

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
    
    def test_get_current_offset_after_writes(self):
        page = Page()
        for i in range(10):
            page.write(i)
            page.increment_record_count()
        assert page.current_offset() == 80

    # can handle writing to a full page
    def test_write_full_page(self):
        page = Page()
        for i in range(page.MAX_RECORDS_PER_PAGE):
            page.write(i)
            page.increment_record_count()
        assert page.has_capacity() == False
        assert page.current_offset() == 4096

    # can handle reading from an invalid offset
    def test_read_invalid_offset(self):
        page = Page()
        for i in range(5):
            page.write(i)
        with pytest.raises(IndexError):
            page.read(-1)
        with pytest.raises(IndexError):
            page.read(5)
        for i in range(5, page.MAX_RECORDS_PER_PAGE):
            page.write(i)
        with pytest.raises(IndexError):
            page.read(4096)

    # can handle updating an invalid offset
    def test_update_invalid_offset(self):
        page = Page()
        with pytest.raises(IndexError):
            page.update_entry(1, 10)

    # can write a new record to the page
    def test_write_record(self):
        page = Page()
        page.write(10)
        page.increment_record_count()
        assert page.num_records == 1
        assert page.data[:COLUMN_DATA_SIZE * page.num_records] == b'\x0A\x00\x00\x00\x00\x00\x00\x00'
        page.write(34550)
        page.increment_record_count()    
        assert page.num_records == 2
        assert page.data[COLUMN_DATA_SIZE * (page.num_records - 1):COLUMN_DATA_SIZE * page.num_records] == b'\xF6\x86\x00\x00\x00\x00\x00\x00'

    # can read a record from the page
    def test_read_record(self):
        page = Page()
        page.write(10)
        page.increment_record_count()
        assert page.read(0) == 10
        page.write(19923940)
        page.increment_record_count()
        assert page.read(1) == 19923940

    # can update an existing record on the page
    def test_update_record(self):
        page = Page()
        page.write(10)
        page.increment_record_count()
        page.update_entry(0, 20)
        assert page.read(0) == 20