import time
from typing import Union
from .config import *

class Page:
    def __init__(self, data: Union[bytearray, None] = None):
        self.num_records = 0
        self.MAX_RECORDS_PER_PAGE = PAGE_DATA_SIZE // COLUMN_DATA_SIZE # 4096 / 8 = 512
        self.bitmap = [0] * self.MAX_RECORDS_PER_PAGE #move to page block level
        self.pinned = 0
        self.dirty = False
        self.timestamp = time.time()
        if data is None:
            self.data = bytearray(PAGE_DATA_SIZE) # 4096 bytes per page
        else:
            self.data = data

    def delete(self, offset_to_delete):
        self.bitmap[offset_to_delete // 8] = 0
        # self.num_records -= 1 # be better to store at the page_block level need to make this work also

    def has_capacity(self) -> bool:
        return self.num_records < self.MAX_RECORDS_PER_PAGE
    
    def current_offset(self) -> int:
        return self.num_records * COLUMN_DATA_SIZE

    def write(self, value_to_write):
        write_offset = self.find_next_open_spot()
        self.bitmap[write_offset // 8] = 1
        if write_offset >= PAGE_DATA_SIZE or write_offset < 0:
            raise IndexError("Attempted to write outside page")
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[write_offset:write_offset + COLUMN_DATA_SIZE] = bytes_to_write
        self.num_records += 1 # might be better to store at the page_block level
        self.dirty = True
        self.timestamp = time.time()
        return True

    def read(self, read_offset) -> int:
        if not self.check_valid_offset(read_offset):
            return None
        bytes_read = self.data[read_offset:read_offset + COLUMN_DATA_SIZE]
        converted_data = int.from_bytes(bytes_read, byteorder='little')
        self.timestamp = time.time()
        return converted_data        
    
    def update_entry(self, offset, value_to_write):
        if self.check_valid_offset(offset) == False:
            raise IndexError("Attempted to update unwritten data")
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[offset:offset + COLUMN_DATA_SIZE] = bytes_to_write

    def check_valid_offset(self, offset):
        if offset > len(self.data) - 1 or offset < 0 :
            raise IndexError("Attempted to access beyond the bounds of the data buffer")
        if offset >= self.num_records * 8:
            return False
        return True
    
    def increment_record_count(self):
        self.num_records += 1
    
    def find_next_open_spot(self):
        bitmap = self.bitmap
        for index in range(len(bitmap)):
            if bitmap[index] == 0:
                return index * 8
        raise ValueError("No open spot found in the bitmap.")
    
    def is_slot_num_valid(self, slot_num):
        return 0 <= slot_num < self.MAX_RECORDS_PER_PAGE
    
    def get_data(self):
        return self.data
    
    def is_dirty(self):
        return self.dirt
    
    def set_dirty(self):
        self.dirty = True

    def get_timestamp(self):
        return self.timestamp
    
    def can_evict(self):
        return self.pinned == 0
    
    def pin_page(self):
        self.pinned += 1

    def unpin_page(self):
        self.pinned -= 1