import uuid
from .config import *

class Page:
    def __init__(self):
        """
        Initializes a new Page instance with a fixed size, ready to store records.
        """
        self.num_records = 0
        self.data = bytearray(PAGE_DATA_SIZE) # 4096 bytes per page
        self.MAX_RECORDS_PER_PAGE = PAGE_DATA_SIZE // COLUMN_DATA_SIZE # 4096 / 8 = 512
        self.record_index = {}
        self.RID = uuid.uuid4()

    def has_capacity(self) -> bool:
        """
        Checks if the page has enough capacity to store at least one more record.
        
        Returns:
        True if there is capacity, False otherwise.
        """
        return self.num_records < self.MAX_RECORDS_PER_PAGE
    
    def current_offset(self) -> int:
        """
        Calculates the current write offset within the page based on the number of records stored.
        
        Returns:
        The byte offset within the page where the next record should be written.
        """
        return self.num_records * COLUMN_DATA_SIZE

    def write(self, value_to_write):
        """
        Writes a new value to the page at the next available offset, updating the page's record index.
        
        Parameters:
        - value_to_write (int): The value to write to the page.
        """
        write_offset = self.current_offset()
        if write_offset >= PAGE_DATA_SIZE:
            raise IndexError("Attempted to write outside page")
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[write_offset:write_offset + COLUMN_DATA_SIZE] = bytes_to_write
        self.record_index[self.num_records] = self.RID 
        self.num_records += 1 # might be better to store at the page_block level

    def read(self, read_offset) -> int:
        """
        Reads and returns the value stored at the specified offset within the page.
        
        Parameters:
        - read_offset (int): The offset from which to read the value.
        
        Returns:
        The value read from the specified offset.
        """
        adjusted_read_offset = read_offset * COLUMN_DATA_SIZE
        self.check_valid_offset(read_offset)
        bytes_read = self.data[adjusted_read_offset:adjusted_read_offset + COLUMN_DATA_SIZE]
        converted_data = int.from_bytes(bytes_read, byteorder='little')
        return converted_data
    
    def update_entry(self, offset, value_to_write):
        """
        Updates the value stored at the specified offset within the page.
        
        Parameters:
        - offset (int): The offset at which to update the value.
        - value_to_write (int): The new value to write to the specified offset.
        """
        self.check_valid_offset(offset)
        adjusted_offset = offset * COLUMN_DATA_SIZE
        bytes_to_write = value_to_write.to_bytes(COLUMN_DATA_SIZE, byteorder='little')
        self.data[adjusted_offset:adjusted_offset + COLUMN_DATA_SIZE] = bytes_to_write

    def check_valid_offset(self, offset):
        """
        Checks if the specified offset is valid for reading or writing within the page.
        
        Parameters:
        - offset (int): The offset to check.
        
        Raises:
        IndexError if the offset is invalid.
        """
        adjusted_offset = offset * COLUMN_DATA_SIZE
        if adjusted_offset + COLUMN_DATA_SIZE > len(self.data) or adjusted_offset < 0 :
            raise IndexError("Attempted to access beyond the bounds of the data buffer")
        if offset >= self.num_records:
            raise IndexError("Attempted to access  an offset with no written data")
    
    def increment_record_count(self):
        """
        Increments the count of records stored in the page.
        """
        self.num_records += 1
