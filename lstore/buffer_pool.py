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