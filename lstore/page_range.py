from .page import Page
from .page_block import PageBlock

class PageRange:
    """
    A PageRange represents a collection of PageBlocks within a table, including both base and tail records.
    It manages the storage, retrieval, and update of records across multiple pages based on the column-store architecture.
    
    Attributes:
        num_columns (int): The number of columns in the table.
        base_pages_per_range (int): The maximum number of base pages within a page range.
        num_records (int): The total number of records within the page range.
        base_pages (list): A list of PageBlocks representing the base records.
        tail_pages (list): A list of PageBlocks representing the tail (update) records.
        tail_page_directory (dict): A mapping of RID to tail page number and offset within that page.
    """
    def __init__(self, num_columns, bufferpool, table_name) -> None:
        """
        Initializes a new instance of the PageRange class.

        Parameters:
            num_columns (int): The number of columns in the table this PageRange is associated with.
        """
        self.num_columns = num_columns
        self.base_pages_per_range = 8
        self.base_pages = []
        self.tail_pages = []     
        # Create base pages with RID, indirection, and schema encoding column
        self.base_pages.append(PageBlock(self.num_columns + 3, bufferpool, self.table_name)) 
        self.tail_page_directory = {}   # rid -> (tail page number, offset)
        # Create tail pages with RID and indirection
        self.tail_pages.append(PageBlock(self.num_columns + 3, bufferpool, self.table_name))
        self.bufferpool = bufferpool
        self.table_name = table_name

    def delete(self, base_page_block_index, base_record_index):
        projected_columns_index = [1] * self.num_columns
        current_tail_rid = self.select_records(base_page_block_index, base_record_index, projected_columns_index)[-2]
        self.base_pages[base_page_block_index].delete(base_record_index)
        if current_tail_rid not in self.tail_page_directory:
            return
        while current_tail_rid != 0:
            current_tail_block, offset_to_delete = self.tail_page_directory[current_tail_rid]
            next_tail_rid = self.tail_pages[current_tail_block].get_record(offset_to_delete, projected_columns_index)[-1]
            self.tail_pages[current_tail_block].delete(offset_to_delete)
            current_tail_rid = next_tail_rid

        # i dont think self.tail_page_directory is having anything removed need to fix


        

    def addNewRecord(self, rid, *columns):
        """
        Adds a new record to the page range. If the last base page is full, a new base page is created.

        Parameters:
            rid (int): The Record ID of the new record.
            *columns: The values for each column in the record.

        Note: This function does not handle overflow or check for existing RIDs.
        """
        if not self.base_pages[-1].has_capacity():
            # last base page full; create new base page
            self.base_pages.append(PageBlock(self.num_columns + 3, self.bufferpool, self.table_name))

        base_page_to_write = self.base_pages[-1]
        
        # Group columns with additional values
        data_to_write = list(columns)  # Convert columns tuple to a list
        data_to_write.extend([rid, 0, 0])  # Append additional values


        
        # Write the data to the base page
        base_page_to_write.write(*data_to_write)

    def has_capacity(self):
        """
        Checks if the current PageRange has capacity for more records.

        Returns:
            bool: True if there is capacity, False otherwise.
        """
        if len(self.base_pages) < self.base_pages_per_range or self.base_pages[-1].has_capacity():
            return True
        return False
    
    def select_records(self, base_page_block_index, record_index, projected_columns_index):
        """
        Selects records matching a given search key from a specific column, projecting only certain columns.

        Parameters:
            search_key: The value to search for in the search_key_column.
            search_key_column (int): The index of the column to search in.
            projected_columns_index (list): A list of column indexes indicating which columns to include in the result.

        Returns:
            list: A list of selected records matching the search criteria.

        Note: Does not directly handle tail records or schema changes.
        """
        record_to_return = []
        # if indirection column is 0
        if self.base_pages[base_page_block_index].column_pages[-2].read(record_index) == 0:
            record_to_return = self.base_pages[base_page_block_index].get_record(record_index, projected_columns_index) # remove indirection
        else:
            latest_record_rid = self.base_pages[base_page_block_index].column_pages[-2].read(record_index)
            # rid -> (tail page number, offset)
            latest_tail_page_number, last_offset = self.tail_page_directory[latest_record_rid]
            record_to_return = self.tail_pages[latest_tail_page_number].get_record(last_offset, projected_columns_index) # remove indirection
        return record_to_return
    
    def updateRecord(self, page_block_index, record_index, new_rid, *columns):
        """
        Updates a record identified by base_rid with new values for specified columns.

        Parameters:
            record_index (int): The index of the record within its page block. [Does not work: This parameter is not used in the body and may be incorrect]
            base_rid (int): The Record ID of the base record to update.
            *columns: New values for the record, with None for columns that are not updated.

        Note: This function assumes the existence of a method to add tail records and update indirection, which may not be fully implemented.
        """
        # Check if the last tail page block has capacity
        if not self.tail_pages or not self.tail_pages[-1].has_capacity():
            self.tail_pages.append(PageBlock(self.num_columns + 3, self.bufferpool, self.table_name))
        
        last_record = []
        last_rid = self.base_pages[page_block_index].column_pages[-2].read(record_index)
        if last_rid in self.tail_page_directory:
            last_tail_page_number, last_offset = self.tail_page_directory[last_rid]
            for i in range(self.num_columns):
               last_record.append(self.tail_pages[last_tail_page_number].column_pages[i].read(last_offset))
        else:
            for i in range(self.num_columns):
                last_record.append(self.base_pages[page_block_index].column_pages[i].read(record_index))

        # Construct the tail record with None for unchanged values and actual values for changed columns
        tail_record = last_record
        for i, column in enumerate(columns):
            if column is not None:
                tail_record[i] = column
        tail_record.extend([new_rid, last_rid, 0])
        
        # Add the tail record to the latest tail page block
        self.tail_pages[-1].write(*tail_record)###########################################################since we have the bitmap need to check pages for open one may be an earlier page that had a delete
        self.tail_page_directory[new_rid] = (len(self.tail_pages) - 1, self.tail_pages[-1].last_written_offset) ###################################################################################

    
    def update_base_record_indirection(self, new_rid, block_index, record_index):
        self.base_pages[block_index].update_base_record_indirection(new_rid, record_index)
        return True
    
    # DOES NOT WORK FULLY
    def update_record_indirection(self, base_rid, new_tail_rid):
        page_index, block_index, record_index = self.find_base_record_location(base_rid)
        if page_index is not None and block_index is not None and record_index is not None:
            # Access the specific PageBlock
            base_page_block = self.base_pages[page_index]
            
            # Update the indirection for the specific record. This requires a new method in PageBlock.
            base_page_block.update_indirection_for_record(record_index, new_tail_rid)
            return True
        else:
            print(f"Base record not found for RID: {base_rid}")
            return False

    # DOES NOT WORK FULLY
    def find_base_record_location(self, base_rid):
        # Check if the RID is in the page directory
        if base_rid in self.page_directory:
            return self.page_directory[base_rid]
        else:
            return None, None, None
    
    # DOES NOT WORK FULLY 
    def find_record_by_rid(self, base_page_num, offset, query_columns):
        record = []

        # Access the base PageBlock
        base_page_block = self.base_pages[base_page_num]

        # Read the schema encoding and indirection values for the record
        schema_encoding = base_page_block.read_schema_encoding(offset)
        indirection = base_page_block.read_indirection(offset)

        # Determine whether to read from the base record or a tail record
        if indirection == 0:
            # No updates, read directly from the base record
            for col_index in query_columns:
                value = base_page_block.read_column_value(col_index, offset)
                record.append(value)
        else:
            # There are updates, locate the latest tail record using indirection
            tail_page_num, tail_offset = self.tail_page_directory[indirection]
            tail_page_block = self.tail_pages[tail_page_num]

            # Read the updated values from the tail record
            for col_index in query_columns:
                if schema_encoding[col_index] == '1':
                    # Column updated, read from tail record
                    value = tail_page_block.read_column_value(col_index, tail_offset)
                else:
                    # Column not updated, read from base record
                    value = base_page_block.read_column_value(col_index, offset)
                record.append(value)

        return record
    
    # DOES NOT WORK FULLY
    def add_tail_record(self, rid, updated_columns):
        if not self.tail_pages or not self.tail_pages[-1].has_capacity():
            # If not, create a new tail page block
            self.tail_pages.append(PageBlock(self.num_columns + 3, self.bufferpool, self.table_name))
        
        # Now, we are sure that the last tail page block has capacity, add the tail record
        self.tail_pages[-1].write_tail_record(rid, updated_columns)
        return True
    
    # DOES NOT WORK FULLY
    
    
    