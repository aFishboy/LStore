from lstore.index import Index
from time import time
from .page_range import PageRange
from avltree import AvlTree
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    """
    A simple class to hold information about a single record within a table.
    """
    def __init__(self, rid, key, columns):
        """
        Initializes a Record object with the specified RID, key, and columns.

        Parameters:
            rid (int): The unique identifier for the record.
            key (int): The key column value for the record.
            columns (list): A list of column values for the record.
        """
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    Represents a table within a database, capable of performing operations such as insert, read, update, and delete on records.
    """
    def __init__(self, name, num_columns, key, bufferpool):
        """
        Initializes the Table with basic information and structures for storing records.

        Parameters:
            name (str): The name of the table.
            num_columns (int): The number of columns in the table.
            key (int): The index of the column that acts as the primary key.
        """
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.data = [] # Note: It seems this is not used
        self.index = Index(self)
        self.page_ranges = [] 
        self.last_base_rid = -1
        self.last_tail_rid = -1
        self.page_directory = {} # maps RID to tuple, (What page_range, !!!NEED TO ALSO HAVE PAGE BLOCK!!!!, what record location in page) 
        # Tracks the location of records within page ranges.
        self.bufferpool = bufferpool

    def insert_record(self, *columns):
        """
        Inserts a new record into the table with the specified column values.

        Parameters:
            *columns: Variable length argument list for column values of the new record.
        """
        total_page_ranges = len(self.page_ranges)
        new_rid = self.generate_base_rid()

        # Check if there is an existing page range with capacity or create a new one
        if total_page_ranges == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.num_columns, self.bufferpool, self.name))
            total_page_ranges += 1

        # Add the new record to the last page range
        self.page_ranges[-1].addNewRecord(new_rid, *columns)#########################################################################since we have the bitmap need to check pages for open one may be an earlier page that had a delete


        # Update the page directory with the new record's location
        self.page_directory[new_rid] = (total_page_ranges - 1, len(self.page_ranges[-1].base_pages) - 1, self.page_ranges[-1].base_pages[-1].last_written_offset)################################################
###########################################################################################herreeeeeeeeeee

        # Update the index for the primary key column with the new RID
        primary_key_value = columns[self.key]

        for i in range(len(columns)):
            avl_tree = self.index.indices[i]
            if columns[i] in avl_tree:
                previous_rids = avl_tree[columns[i]]
            else:
                previous_rids = []  
            previous_rids.append(new_rid)
            avl_tree[columns[i]] = previous_rids

    def update_record(self, primary_key, *columns):
        """
        Updates the record with the given primary key to have the new column values specified.
        Only columns corresponding to non-None values in the *columns parameter are updated.

        Parameters:
            primary_key: The primary key of the record to update.
            *columns: New values for the record's columns, with None indicating no change.

        Returns:
            bool: True if the update was successful, False otherwise.
        
        Note: This method requires thorough testing and validation to ensure correct functionality.
        """
        # self.index.
        base_rid = self.index.locate(self.key, primary_key)[0]
        if base_rid is None:
            print("Record with primary key not found.")
            return False
        
        # Retrieve the page range and record index from the page directory using the located RID
        page_range_index, page_block_index, record_index = self.page_directory[base_rid]

        projected_columns_index = [1] * self.num_columns
        old_record_entries_to_remove = self.select_records(primary_key, self.key, projected_columns_index)[0].columns
        for i, value in enumerate(columns):
            if value != None:
                avl_tree = self.index.indices[i]
                previous_rids = avl_tree[old_record_entries_to_remove[i]]
                previous_rids.remove(base_rid)
                avl_tree[old_record_entries_to_remove[i]] = previous_rids




        # Assuming you have a list of PageRange objects in self.page_ranges
        # Call the updateRecord method on the appropriate PageRange object with the located info
        new_rid = self.generate_tail_rid()
        self.page_ranges[page_range_index].updateRecord(page_block_index, record_index, new_rid, *columns)
        for i in range(len(columns)):
            avl_tree = self.index.indices[i]
            if columns[i] != None:
                if columns[i] in avl_tree:
                    previous_rids = avl_tree[columns[i]]
                else:
                    previous_rids = []  
                previous_rids.append(base_rid)

                avl_tree[columns[i]] = previous_rids

        self.page_ranges[page_range_index].update_base_record_indirection(new_rid, page_block_index, record_index)

        # Update the indirection pointer in the base record
        return True
    
    
    # The method below is marked as "DOES NOT WORK" - specifics of record location unpacking need revision.
    def read_record(self, rid, query_columns):
        """
        Reads the values of the specified columns for the record with the given RID.

        Parameters:
            rid (int): The RID of the record to read.
            query_columns (list[int]): Indices of the columns to return values for.

        Returns:
            Record: A Record object containing the requested data, or None if the record cannot be found.

        Note: Requires correct handling of record location unpacking and validation.
        """
        # Check if the RID exists in the page directory
        if rid not in self.page_directory:
            print(f"Record with RID {rid} not found.")
            return None

        page_range_index, base_page_index = self.page_directory[rid]

        # Access the corresponding PageRange object
        if page_range_index >= len(self.page_ranges) or base_page_index >= len(self.page_ranges[page_range_index].base_pages):
            print(f"Page range or base page index out of bounds.")
            return None

        page_range = self.page_ranges[page_range_index]
        base_page = page_range.base_pages[base_page_index]

        # Assuming you have a method to iterate over records in the base_page and find one by RID
        record_data, record_offset = base_page.find_record_by_rid(rid)  
        if record_data is None:
            print(f"Record with RID {rid} not found in the specified base page.")
            return None

        # If your record data includes all columns, extract only the requested ones
        # This is simplified; you might need to adjust based on your data structure
        record_columns = [record_data[col] for col in query_columns]

        key_value = record_columns[self.key] if self.key < len(record_columns) else None
        return Record(rid, key_value, record_columns)

    
    def select_records(self, search_key, search_key_column, projected_columns_index):
        """
        Selects records from the table based on a search key and specified column indexes.

        Parameters:
            search_key: The value to search for in the specified search_key_column.
            search_key_column: The index of the column to search through.
            projected_columns_index: A list of column indexes indicating which columns to return.

        Returns:
            A list of Record objects containing the data for each selected record.
        """
        base_rids = self.index.locate(search_key_column, search_key)
        if base_rids == None:
            return None
        selected_records = []
        for base_rid in base_rids:
            page_range_index, page_block_index, record_index = self.page_directory[base_rid]
            returned_records = self.page_ranges[page_range_index].select_records(page_block_index, record_index, projected_columns_index)[:-1]
            selected_records.append(returned_records)

        record_object_array = []
        for record in selected_records:
            record_object_array.append(Record(record[-1], self.key, record[:len(record) - 1]))

        return record_object_array


    def delete_record(self, key):
        """
        Deletes a record from the table based on the provided key.

        Parameters:
            key: The key of the record to be deleted.

        Returns:
            None. Prints a message if the record does not exist.
        """
        base_rid = self.index.locate(self.key, key)[0]
        projected_columns_index = [1] * self.num_columns
        record_to_delete = self.select_records(key, self.key, projected_columns_index)[0].columns

        # go to each avl index tree and remove rid associated with each key
        for i in range(self.num_columns):
            avl_tree = self.index.indices[i]
            previous_rids = avl_tree[record_to_delete[i]]
            previous_rids.remove(base_rid)
            avl_tree[record_to_delete[i]] = previous_rids

        page_range_index, page_block_index, record_index = self.page_directory[base_rid]
        self.page_ranges[page_range_index].delete(page_block_index, record_index)
        del self.page_directory[base_rid]

    
    def record_exists(self, key): 
        """
        Checks if a record with the given key exists in the table.

        Parameters:
            key: The key to check for existence.

        Returns:
            True if the record exists, False otherwise.
        """
        # Check if the record exists in the index
        return self.index.locate(self.key, key) != -1
        

    def is_locked(self, key):
        locked_records = {
        1: True,  # Example of a locked record with key 1
        2: False  # Example of an unlocked record with key 2
        # Add more records as needed
         }
        if key in locked_records:
            return locked_records[key]
        else:
            return False  # Default assumption: record is not locked if not found in the dictionary

    def generate_base_rid(self):
        """
        Generates a unique record identifier (RID) for new records.

        Returns:
            An integer representing the new RID.
        """
        self.last_base_rid += 1
        return self.last_base_rid
    
    def generate_tail_rid(self):
        """
        Generates a unique record identifier (RID) for new records.

        Returns:
            An integer representing the new RID.
        """
        self.last_tail_rid += 1
        return self.last_tail_rid
    
    
    def update_indirection(self, base_rid, new_tail_rid):
        """
        Updates the indirection pointer for a base record to point to a new tail record.

        Parameters:
            base_rid: The RID of the base record to update.
            new_tail_rid: The RID of the new tail record that the base record should point to.

        Returns:
            None. Updates the indirection column of the base record within the specified page range.
        """
        # Assuming the page_directory maps RIDs to their page range and page block locations
        page_range_index, page_block_index = self.page_directory[base_rid]
        page_range = self.page_ranges[page_range_index]

        # Call a method on the page range to update the indirection
        page_range.update_base_record_indirection(base_rid, new_tail_rid)


    def add_tail_record(self, base_rid, updated_columns):
        """
        Adds a tail record with updated column values for an existing base record.

        Parameters:
            base_rid: The RID of the base record to update.
            updated_columns: A list of new values for the specified columns of the base record.

        Returns:
            True if the tail record was successfully added, False otherwise.
        """
        # Locate the base record and its page range.
        print(f"Type of base_rid: {type(base_rid)}, Value: {base_rid}")
        page_range_index, _ = self.page_directory[base_rid]
        page_range = self.page_ranges[page_range_index]

        # Create and add the new tail record.
        tail_rid = self.generate_tail_rid()
        print(f"Type of new_tail_rid: {type(tail_rid)}, Value: {tail_rid}")
        success = page_range.add_tail_record(tail_rid, updated_columns)
        if not success:
            return False

        # Update the indirection column of the base record.
        self.update_indirection(base_rid, tail_rid)
        return True

    def get_all_records(self):
        """
        Returns all records in the table.

        Returns:
            A list of all records in the table.
        """
        all_records = []
        for page_range in self.page_ranges:
            for base_page in page_range.base_pages:
                for record in base_page.records:
                    all_records.append(record)
        return all_records
    
    def __merge(self):
        #print("merge is happening")
        pass
 
