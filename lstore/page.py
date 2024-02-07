from .config import *
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_DATA_SIZE) # 4096 bytes per page
        self.MAX_RECORDS_PER_PAGE = PAGE_DATA_SIZE // COLUMN_DATA_SIZE # 4096 / 8 = 512

    def has_capacity(self) -> bool:
        return self.num_records < self.MAX_RECORDS_PER_PAGE
    
    def current_offset(self) -> int:
        return self.num_records * COLUMN_DATA_SIZE

    def write(self, value_to_write):
        write_offset = self.current_offset()
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[write_offset:write_offset + COLUMN_DATA_SIZE] = bytes_to_write
        self.num_records += 1
        print(self.num_records)

    def read(self, read_offset) -> int:
        adjusted_read_offset = read_offset * COLUMN_DATA_SIZE
        bytes_read = self.data[adjusted_read_offset:adjusted_read_offset + COLUMN_DATA_SIZE]
        converted_data = int.from_bytes(bytes_read, byteorder='little')
        return converted_data
    
    def update_entry(self, offset, value_to_write):
        adjusted_offset = offset * self.COL_DATA_SIZE
        bytes_to_write = value_to_write.to_bytes(self.COL_DATA_SIZE, byteorder='little')
        self.data[adjusted_offset:adjusted_offset + self.COL_DATA_SIZE] = bytes_to_write