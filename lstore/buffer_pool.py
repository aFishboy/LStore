import base64
import linecache
import os
from lstore.page_range import PageRange
from lstore.page_range_serializer import PageRangeSerializer
from .config import *
import msgpack
# change to pull in page ranges not just pages 
class BufferPool:
    def __init__(self, path, table_name, num_columns, last_page_range, total_page_ranges, base_path):
        self.buffer_pool_size = BUFFERPOOL_SIZE
        self.buffer_pages = {}  # page id -> page range object
        self.table_name = table_name
        self.total_page_ranges = total_page_ranges
        self.num_columns = num_columns
        self.last_page_range = last_page_range
        self.path = path
        self.base_path = base_path
    
    def write_value(self, page_range_id, value_to_write):
        current_page_range = self.get_page(page_range_id)
        if current_page_range == None:
            current_page_range = self.add_page(page_range_id)

        current_page_range.pin_page()
        current_page_range.set_dirty()
        current_page_range.write(value_to_write)

        current_page_range.unpin_page()
        return True
    
    def add_page_range(self, current_page_range, page_range_id):
        self.evict_if_bufferpool_full()
        self.buffer_pages[page_range_id] = current_page_range
    
    def get_page_range(self, page_range_id):
        # print("get page range!!!!!!!!!!!!!!!!!!!!!!!!!")
        if page_range_id in self.buffer_pages:
            return self.buffer_pages[page_range_id]
        if self.total_page_ranges - 1 < page_range_id:
            return None
        
        self.evict_if_bufferpool_full()
        current_page_range = self.get_page_range_from_file(page_range_id)
        self.buffer_pages[page_range_id] = current_page_range
        return current_page_range
    
    def get_page_range_to_insert(self, last_page_range):
        if last_page_range in self.buffer_pages:
             current_page_range = self.buffer_pages[last_page_range]
        else:
            self.get_page_range_from_file(last_page_range)
        if current_page_range.has_capacity():
            return current_page_range
        else:
            return None

    def get_page_range_from_file(self, page_range_id):
        os.chdir(self.path)
        # print("page range id:", page_range_id)
        file_offset = 5 + page_range_id  # Check if this offset calculation is correct
        # try:
        table_file_page_range_name = "page_range_" + str(page_range_id) + "_" + self.table_name
        # print("table_file_page_range_name", table_file_page_range_name)
        with open(table_file_page_range_name, 'rb') as file:  # Open the file in binary mode
            serialized_page_range = file.read()  # Read the entire contents of the file

        # print("serialized:", serialized_page_range, file_offset)
        # Decode the base64 encoded string back to bytes
        # decoded_serialized_data = base64.b64decode(serialized_page_range)
        # Deserialize the page range data
        page_range = PageRangeSerializer.deserialize_page_range(serialized_page_range)
        os.chdir(self.base_path)
        return page_range
        # except Exception as e:
        #     print("Error get page range from file:", e)
        #     return None  # Return None or raise a specific exception depending on your error handling strategy


                
    """Check Implementation"""
    def evict_page_range(self):
        for page_range_id, page_range in list(self.buffer_pages.items()):
            if not page_range.is_pinned():
                if page_range.is_dirty():
                    self.write_page_range(page_range_id, page_range)  # Write back to disk if modified.
                    page_range.set_clean()  # Reset dirty flag after writing.
                del self.buffer_pages[page_range_id]
                return
        raise Exception("No unpinned pages available to evict.")
        
    def evict_all_page_ranges(self):
        while self.buffer_pages:
            self.evict_page_range()

    def write_page_range(self, page_range_id, page_range):
        os.chdir(self.path)
        # Serialize the page range data
        serialized_data = PageRangeSerializer.serialize_page_range(page_range)
        # print("page range write to file", page_range)
        # Encode the serialized data to a string
        # encoded_serialized_data = base64.b64encode(serialized_data).decode('utf-8')
        # # test this ^^^^^^
        

        # # Construct the line to write to the file
        # line_to_write = f"{encoded_serialized_data}\n"

        # Open the file and write the data to the specified line
        table_file_name = "page_range_" + str(page_range_id) + "_" + page_range.table_name # Assuming the file name is based on the table name
        with open(table_file_name, 'wb') as file:
            file.write(serialized_data)
        os.chdir(self.base_path)
        # with open(table_file_name, 'r+', encoding='utf-8') as file:
        #     # Move the file cursor to the appropriate line
        #     # for _ in range(3 + page_range_id):
        #     #     file.readline()  # Read and discard lines until reaching the desired line
        #     # # Write the data to the specified line
        #     file.write(line_to_write)
    
    def has_capacity(self):
        if (self.buffer_pool_size - len(self.buffer_pages)) <= 0:
            return False
        return True
    
    def evict_if_bufferpool_full(self):
        if not self.has_capacity():
            self.evict_page()


    def get_base_page_range(self, start, end):
        # Dictionary to hold the base pages loaded into memory
        base_pages = {}
        # Loop through the range of page IDs
        for page_range_id in range(start, end + 1):
            # Check if the page is a base page
            if self.disk.page_is_base(page_range_id):
                # Load the base page into the buffer pool
                base_pages[page_range_id] = self.get_page(page_range_id)
        # Return the dictionary of loaded base pages
        return base_pages


    def update_base_page_range(self, base_pages):
        # base_pages: dict of page_range_id -> Page objects
        for page_range_id, page in base_pages.items():
            if page.is_dirty():
                self.disk.write_page(page_range_id, page)  # Write back to disk if modified.
                page.set_clean()  # Reset dirty flag after writing.

    def get_tps(self, table_name, column_id, page_range):
        return self.disk.get_tps(table_name, column_id, page_range)

    def update_tps(self, table_name, column_id, page_range, tps):
        # self.tps is a dictionary where each table_name keys to another dictionary,
        # which then keys from column_id to a tuple of (page_range, tps value)
        # This updates the TPS value for a specific table, column, and page range
        if table_name not in self.tps:
            self.tps[table_name] = {}
        self.tps[table_name][column_id] = (page_range, tps)

    def copy_tps(self, old_tps):
        self.tps = old_tps

    def get_latest_tail(self, table_name):
        # Assuming self.latest_tail is a dictionary mapping table names to the ID of their latest tail record
        # This function returns the latest tail record ID for a given table
        return self.latest_tail.get(table_name, None)

    def set_latest_tail(self, table_name, tail_id):
        # Updates the ID of the latest tail record for a given table
        self.latest_tail[table_name] = tail_id


    def merge(self):
        for page_range_id, page_range in self.buffer_pages.items():
            base_page = page_range.get_base_page()
            if page_range.has_tail_pages():
                for tail_page in page_range.get_tail_pages():
                    if base_page:
                        self.merge_tail_into_base(tail_page, base_page)
    
    def merge_tail_into_base(tail_page, base_page):
        for records in tail_page.records():
            base_page.add_record(records)