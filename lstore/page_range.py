from lstore.page import Page
from lstore.page_block import PageBlock

class PageRange:
    def __init__(self, num_columns) -> None:
        """
        Initializes a new PageRange instance.
        
        Parameters:
        - num_columns (int): The number of columns in the table this PageRange belongs to.
        """
        self.num_columns = num_columns
        self.base_pages_per_range = 8
        self.num_records = 0
        self.base_pages = []  # List of base page blocks
        self.tail_pages = []  # List of tail page blocks
        self.tail_page_directory = {}  # Directory for tail page redirections

        # Create initial base page with RID, indirection, and schema encoding columns
        self.base_pages.append(PageBlock(self.num_columns + 3)) 

    def addNewRecord(self, rid, *columns):
        """
        Adds a new record to the PageRange. If the last base page does not have enough capacity,
        a new base page block is created and added to the PageRange.
        
        Parameters:
        - rid (int): The record ID of the new record.
        - columns (tuple): The column values of the new record.
        """
        self.num_records += 1
        if not self.base_pages[-1].has_capacity():
            self.base_pages.append(PageBlock(self.num_columns + 3))  # Create new base page

        base_page_to_write = self.base_pages[-1]
        data_to_write = list(columns) + [rid, 0, 0]  # Append RID, indirection, and schema encoding
        base_page_to_write.write(*data_to_write)

    def update_record(self, rid, *updated_columns):
        """
        Updates the record identified by rid with the new values provided in updated_columns.
        
        Parameters:
        - rid (int): The record ID of the record to update.
        - updated_columns (tuple): The new values for the record, with None for columns that are not updated.
        """
        pass
    
    
    def readRecord(self, base_page_num, offset, query_columns):
        """
        Reads and returns the values of specified columns for a record located in a base page or
        a tail page if the record has been updated. Handles indirection and schema encoding.
        
        Parameters:
        - base_page_num (int): The index of the base page from which to read the record.
        - offset (int): The offset within the page where the record starts.
        - query_columns (list of int): The indices of the columns to read.
        
        Returns:
        A list of values for the specified columns.
        """
        record = []

        if base_page_num >= len(self.base_pages):
            raise ValueError(f"Base page number {base_page_num} out of range.")

        base_page_block = self.base_pages[base_page_num]

        # Assuming indirection and schema encoding are stored in the last two positions
        indirection = base_page_block.column_pages[self.num_columns + 1].read(offset)
        schema_encoding = base_page_block.column_pages[self.num_columns + 2].read(offset)
        schema_encoding_bin = format(schema_encoding, '0{}b'.format(self.num_columns))

        for col in query_columns:
            if col >= self.num_columns:
                raise ValueError(f"Query column {col} out of range.")

            if indirection != 0 and col in self.tail_page_directory:
                # Handling for cases where indirection might point to a non-existent tail page
                tail_page_info = self.tail_page_directory.get(indirection)
                if tail_page_info is None:
                    raise ValueError(f"Indirection value {indirection} points to a non-existent tail page.")

                tail_page_num, tail_offset = tail_page_info
                if tail_page_num >= len(self.tail_pages):
                    raise ValueError(f"Tail page number {tail_page_num} out of range.")

                tail_page = self.tail_pages[tail_page_num]

                if schema_encoding_bin[col] == '1':
                    # Read from tail if updated
                    value = tail_page.column_pages[col].read(tail_offset)
                else:
                    # Otherwise, read from base
                    value = base_page_block.column_pages[col].read(offset)
            else:
                # No indirection, read directly from base
                value = base_page_block.column_pages[col].read(offset)

            record.append(value)

        return record

    
    def current_record_count(self):
        return self.num_records
    
    def has_capacity(self):
        """
        Checks if the current PageRange has capacity to add more records to its base pages.
        
        Returns:
        True if there is capacity for more records, False otherwise.
        """
        if len(self.base_pages) < self.base_pages_per_range or self.base_pages[-1].has_capacity():
            return True
        return False