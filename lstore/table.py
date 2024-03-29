import threading
from lstore.buffer_pool import BufferPool
from lstore.index import Index
from time import time
from lstore.page_block import PageBlock
from .page_range import PageRange
from avltree import AvlTree
import msgpack
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
    def __init__(self, name, num_columns, key, total_page_ranges, rid_gen, last_page_range, path, base_path):
        """
        Initializes the Table with basic information and structures for storing records.

        Parameters:
            name (str): The name of the table.
            num_columns (int): The number of columns in the table.
            key (int): The index of the column that acts as the primary key.
        """
        self.name = name
        self.num_columns = num_columns
        self.key = key
        self.index = None #self.read_index() 
        self.page_directory = {} # maps RID to tuple, (What page_range, !!!NEED TO ALSO HAVE PAGE BLOCK!!!!, what record location in page)
        self.total_page_ranges = total_page_ranges
        self.rid_gen = rid_gen
        # Tracks the location of records within page ranges.
        self.last_page_range = last_page_range
        self.path = path
        self.base_path = base_path
        self.bufferpool = BufferPool(self.path, self.name, self.num_columns, self.last_page_range, self.total_page_ranges, self.base_path)
        self.index_columns = [key]
        self.lock = threading.Lock()

    def __str__(self):
        return f"Table: {self.name}, Columns: {self.num_columns}, Primary Key Index: {self.key}"

    def insert_record(self, *columns):
        """
        Inserts a new record into the table with the specified column values.

        Parameters:
            *columns: Variable length argument list for column values of the new record.
        """
        # print("here!!!!!!!!!!!!!!!!!!!!!!!!!!")
        new_rid = self.rid_gen.generate_base_rid()
        if self.last_page_range == -1:
            # create new page range

            current_page_range = PageRange(self.num_columns, self.name, self.total_page_ranges)
            self.total_page_ranges +=1
            self.last_page_range += 1
            self.bufferpool.add_page_range(current_page_range, self.last_page_range)
        else:
            # get page range from bufferpool
            # print("self.last_page_range!!!!!!!!!!!!!!!!!!!!!!", self.last_page_range, type(self.last_page_range))
            current_page_range = self.bufferpool.get_page_range_to_insert(self.last_page_range)
            if current_page_range == None:
                #if self.bufferpool.has_capacity():
                current_page_range = PageRange(self.num_columns, self.name, self.total_page_ranges)
                self.total_page_ranges +=1
                self.last_page_range += 1
                self.bufferpool.add_page_range(current_page_range, self.last_page_range)
                #else:
            #else:
                #print("error due to this")

        current_page_range.addNewRecord(new_rid, *columns)
        self.page_directory[new_rid] = (self.total_page_ranges - 1, len(current_page_range.base_pages) - 1, current_page_range.base_pages[-1].last_written_offset)

        # Update the index for the primary key column with the new RID
        # primary_key_value = columns[self.key]

        for i in range(len(columns)):
            if i not in self.index_columns:
                continue
            avl_tree = self.index.indices[i]
            if columns[i] in avl_tree:
                previous_rids = avl_tree[columns[i]]
            else:
                previous_rids = []  
            previous_rids.append(new_rid)
            avl_tree[columns[i]] = previous_rids
    
    def read_index(self, file_name, disk):
        # read index return a list of AvlTree objects and pass that list
        if file_name == None:
            list_of_AvlTrees = [AvlTree() for _ in range(self.num_columns)]
        else:
            list_of_AvlTrees = disk.read_index(file_name, self.num_columns)
        self.index = Index(self, list_of_AvlTrees)

    def acquire_lock(self):
        return self.lock.acquire(timeout=0)  
        
    def release_lock(self):
        if self.lock.locked():
            self.lock.release()



    
    def read_page_directory(self, file_name, disk):
        if file_name == None:
            page_directory_string = {}
        else:
            # read index return a list of AvlTree objects and pass that list
            page_directory_string = disk.read_page_directory(file_name)
            # print(page_directory_string)
            self.page_directory = eval(page_directory_string)

    def evict_bufferpool(self):
        self.bufferpool.evict_all_page_ranges()



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
        base_rid_list = self.index.locate(self.key, primary_key)
        if len(base_rid_list) == 0:
            return base_rid_list
        
        base_rid = base_rid_list[0]
        
        # Retrieve the page range and record index from the page directory using the located RID
        page_range_index, page_block_index, record_index = self.page_directory[base_rid]

        projected_columns_index = [1] * self.num_columns
        old_record_entries_to_remove = self.select_records(primary_key, self.key, projected_columns_index)[0].columns
        for i, value in enumerate(columns):
            if value != None and i in self.index_columns:
                avl_tree = self.index.indices[i]
                previous_rids = avl_tree[old_record_entries_to_remove[i]]
                previous_rids.remove(base_rid)
                avl_tree[old_record_entries_to_remove[i]] = previous_rids




        # Assuming you have a list of PageRange objects in self.page_ranges
        # Call the updateRecord method on the appropriate PageRange object with the located info
        new_rid = self.generate_tail_rid()
        self.bufferpool.get_page_range(page_range_index).updateRecord(page_block_index, record_index, new_rid, *columns)
        for i in range(len(columns)):
            if i not in self.index_columns:
                continue
            avl_tree = self.index.indices[i]
            if columns[i] != None:
                if columns[i] in avl_tree:
                    previous_rids = avl_tree[columns[i]]
                else:
                    previous_rids = []  
                previous_rids.append(base_rid)

                avl_tree[columns[i]] = previous_rids

        self.bufferpool.get_page_range(page_range_index).update_base_record_indirection(new_rid, page_block_index, record_index)

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
        if page_range_index >= len(self.bufferpool.get_page_range(page_range_index)) or base_page_index >= len(self.bufferpool.get_page_range(page_range_index).base_pages):
            print(f"Page range or base page index out of bounds.")
            return None

        page_range = self.bufferpool.get_page_range(page_range_index)
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
        if search_key_column not in self.index_columns:
            return []
        
        base_rids = self.index.locate(search_key_column, search_key)
        if base_rids == None:
            return []
        selected_records = []
        for base_rid in base_rids:
            page_range_index, page_block_index, record_index = self.page_directory[base_rid]
            returned_records = self.bufferpool.get_page_range(page_range_index).select_records(page_block_index, record_index, projected_columns_index)[:-1]
            
            
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
            if i not in self.index_columns:
                continue
            avl_tree = self.index.indices[i]
            previous_rids = avl_tree[record_to_delete[i]]
            previous_rids.remove(base_rid)
            avl_tree[record_to_delete[i]] = previous_rids

        page_range_index, page_block_index, record_index = self.page_directory[base_rid]
        self.bufferpool.get_page_range(page_range_index).delete(page_block_index, record_index)
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

    def create_index(self, index_column_to_create):
        if index_column_to_create in self.index_columns:
            raise ValueError("Error: Cannot create duplicate index.")
        self.index_columns.append(index_column_to_create)

        found_rids = set()
        key_avl_tree = self.index.indices[self.key]
        projected_columns_index = [0] * self.num_columns
        projected_columns_index[index_column_to_create] = 1
        new_avl_tree = self.index.indices[index_column_to_create]
        for rid_list in key_avl_tree.values():
            for rid in rid_list:
                if rid not in found_rids:
                    found_rids.add(rid)
                    page_range_index, page_block_index, record_index = self.page_directory[rid]
                    returned_val = self.bufferpool.get_page_range(page_range_index).select_records(page_block_index, record_index, projected_columns_index)[0]

                    if returned_val in new_avl_tree:
                        previous_rids = new_avl_tree[returned_val]
                    else:
                        previous_rids = []  
                    previous_rids.append(rid)
                    new_avl_tree[returned_val] = previous_rids

    def drop_index(self, index_column_to_drop):
        pass
        # if index_column_to_drop in self.index_columns:
        #     self.index_columns.remove(index_column_to_drop)
        #     self.index.indices[index_column_to_drop] = AvlTree()
                    


    def generate_base_rid(self):
        """
        Generates a unique record identifier (RID) for new records.

        Returns:
            An integer representing the new RID.
        """
        # self.last_base_rid += 1
        return self.rid_gen.generate_base_rid()
    
    def generate_tail_rid(self):
        """
        Generates a unique record identifier (RID) for new records.

        Returns:
            An integer representing the new RID.
        """
        
        # self.last_tail_rid += 1
        return self.rid_gen.generate_tail_rid()
    
    
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
        page_range = self.bufferpool.get_page_range(page_range_index)
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
        # print(f"Type of base_rid: {type(base_rid)}, Value: {base_rid}")
        page_range_index, _ = self.page_directory[base_rid]
        page_range = self.bufferpool.get_page_range(page_range_index)

        # Create and add the new tail record.
        tail_rid = self.generate_tail_rid()
        # print(f"Type of new_tail_rid: {type(tail_rid)}, Value: {tail_rid}")
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
        for page_range in self.total_page_ranges:
            for base_page in page_range.base_pages:
                for record in base_page.records:
                    all_records.append(record)
        return all_records
    
    def merge(self):
        self.bufferpool.merge()

    
 
