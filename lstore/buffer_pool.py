import os
from lstore.disk import Disk
from lstore.page import Page
from .config import *

class BufferPool:
    def __init__(self, size, path):
        if path != "":
            os.makedirs(path, exist_ok=True)
        self.buffer_pool_size = BUFFERPOOL_SIZE
        self.buffer_pages = {}  # page id -> page object
        self.disk = Disk(path)
    
    def write_value(self, page_id, value_to_write):
        current_page = self.get_page(page_id)
        if current_page == None:
            current_page = self.add_page(page_id)

        current_page.pin_page()
        current_page.set_dirty()
        current_page.write(value_to_write)

        current_page.unpin_page()
        return True
    
    def add_page(self, page_id):
        self.evict_if_bufferpool_full()
        current_page = Page()
        self.buffer_pages[page_id] = current_page
        return current_page
    
    def get_page(self, page_id):
        if page_id in self.buffer_pages:
            return self.buffer_pages[page_id]
        if not self.disk.page_exists(page_id):
            return None
        
        self.evict_if_bufferpool_full()
        current_page = self.disk.get_page(page_id)
        self.buffer_pages[page_id] = current_page
        return current_page
        
    """         NEED TO IMPLEMENT       """#
    def evict_page(self):
        pass
        
    def evict_all_pages(self):
        while self.buffer_pages:
            self.evict_page()
    
    def has_capacity(self):
        if (self.buffer_pool_size - len(self.buffer_pages)) <= 0:
            return False
        return True
    
    """PROBABLY NEED TO CHECK IF ALL PAGES ARE PINNED AND CANT EVICT BUT MAY NOT BECAUSE VERY UNLIKELY"""
    def evict_if_bufferpool_full(self):
        if not self.has_capacity():
            self.evict_page()

    """         NEED TO IMPLEMENT (Tristan)     """
    """ This section is for merging base pages and tail pages
    Simplest way is to load a copy of all base pages in selected range into memory
    We can add an extra column to the database, the Base RID column
    Two copies of the base pages will be kept in memory
    Implementation of TPS
    Frequency of merging is up to us though"""

    def get_base_page_range(self, start, end):
        base_pages = {}
        for page_id in range(start, end + 1):
            if self.disk.page_is_base(page_id):
                base_pages[page_id] = self.get_page(page_id)
        return base_pages

    def update_base_page_range(self):
        # base_pages: dict of page_id -> Page objects
        for page_id, page in base_pages.items():
            if page.is_dirty():
                self.disk.write_page(page_id, page)  # Write back to disk if modified.
                page.set_clean()  # Reset dirty flag after writing.

    def get_tps(self, table_name, column_id, page_range):
        return self.disk.get_tps(table_name, column_id, page_range)

    def update_tps(self, table_name, column_id, page_range, tps):
        pass

    def copy_tps(self, old_tps):
        self.tps = old_tps

    def get_latest_tail(self):
        pass

    def set_latest_tail(self):
        pass