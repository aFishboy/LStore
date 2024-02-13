import uuid
from .config import *

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_DATA_SIZE) # 4096 bytes per page
        self.MAX_RECORDS_PER_PAGE = PAGE_DATA_SIZE // COLUMN_DATA_SIZE # 4096 / 8 = 512
        self.record_index = {}
        self.RID = uuid.uuid4()

    def has_capacity(self) -> bool:
        return self.num_records < self.MAX_RECORDS_PER_PAGE
    
    def current_offset(self) -> int:
        return self.num_records * COLUMN_DATA_SIZE

    def write(self, value_to_write):
        write_offset = self.current_offset()
        if write_offset >= PAGE_DATA_SIZE or write_offset < 0:
            raise IndexError("Attempted to write outside page")
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[write_offset:write_offset + COLUMN_DATA_SIZE] = bytes_to_write
        self.record_index[self.num_records] = self.RID 
        self.num_records += 1 # might be better to store at the page_block level

    def read(self, read_offset) -> int:
        if not self.check_valid_offset(read_offset):
            return None
        bytes_read = self.data[read_offset:read_offset + COLUMN_DATA_SIZE]
        converted_data = int.from_bytes(bytes_read, byteorder='little')
        return converted_data        
    
    def update_entry(self, offset, value_to_write):
        self.check_valid_offset(offset)
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[offset:offset + COLUMN_DATA_SIZE] = bytes_to_write

    def check_valid_offset(self, offset):
        if offset + COLUMN_DATA_SIZE > len(self.data) or offset < 0 :
            return False
        if offset >= self.num_records * 8:
            return False
        return True
    
    def increment_record_count(self):
        self.num_records += 1
    
    def find_key_offsets(self, search_key):
        found_key_offsets = []
        end_of_written_data_offset = self.current_offset()
        current_offset = 0
        while current_offset < end_of_written_data_offset:
            read_data = self.read(current_offset)
            if read_data == search_key:
                found_key_offsets.append(current_offset)
            current_offset += 8
        return found_key_offsets