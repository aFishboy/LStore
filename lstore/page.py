from .config import *
import uuid
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
        if write_offset >= PAGE_DATA_SIZE:
            raise IndexError("Attempted to write outside page")
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[write_offset:write_offset + COLUMN_DATA_SIZE] = bytes_to_write
        self.record_index[self.num_records] = self.RID 
        self.num_records += 1
        

    def read(self, read_offset) -> int:
        adjusted_read_offset = read_offset * COLUMN_DATA_SIZE
        self.check_valid_offset(read_offset)
        bytes_read = self.data[adjusted_read_offset:adjusted_read_offset + COLUMN_DATA_SIZE]
        converted_data = int.from_bytes(bytes_read, byteorder='little')
        return converted_data
    
    def update_entry(self, offset, value_to_write):
        self.check_valid_offset(offset)
        adjusted_offset = offset * COLUMN_DATA_SIZE
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[adjusted_offset:adjusted_offset + COLUMN_DATA_SIZE] = bytes_to_write

    def check_valid_offset(self, offset):
        adjusted_offset = offset * COLUMN_DATA_SIZE
        if adjusted_offset + COLUMN_DATA_SIZE > len(self.data) or adjusted_offset < 0 :
            raise IndexError("Attempted to access beyond the bounds of the data buffer")
        if offset >= self.num_records:
            raise IndexError("Attempted to access  an offset with no written data")
    