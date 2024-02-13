from lstore.index import Index
from time import time
from lstore.page_range import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    Initializes a new Table instance.
    
    Parameters:
    - name (str): The name of the table.
    - num_columns (int): The number of columns in the table.
    - key (int): The index of the column that acts as the primary key.
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.data = []
        self.index = Index(self)
        self.page_ranges = [] 
        self.last_rid = -1
        self.page_directory = {} # maps RID to tuple, (What page_range, what record location in page) 

    def insert_record(self, *columns):
        """
        Inserts a new record into the table. The method determines the correct PageRange to use
        (either the last one if there's enough capacity or creates a new one) and adds the record there.
        
        Parameters:
        - columns (tuple): The values for each column in the record to be inserted.
        """
        total_page_ranges = len(self.page_ranges)
        new_rid = self.generate_rid()

        if total_page_ranges != 0 and self.page_ranges[-1].has_capacity():
            self.page_ranges[-1].addNewRecord(new_rid, *columns)
        else:
            self.page_ranges.append(PageRange(self.num_columns))
            total_page_ranges += 1
            self.page_ranges[-1].addNewRecord(new_rid, *columns)

        # Update the index for the key column
        key_value = columns[self.key]
        if key_value in self.index.indices[self.key]:
            self.index.indices[self.key][key_value].append(new_rid)
        else:
            self.index.indices[self.key][key_value] = [new_rid]

        # Optionally, update indices for other columns if needed
        # for col_index, col_value in enumerate(columns):
        #     if col_index != self.key:  # Skip the key column itself
        #         if col_value in self.index.indices[col_index]:
        #             self.index.indices[col_index][col_value].append(new_rid)
        #         else:
        #             self.index.indices[col_index][col_value] = [new_rid]

        # Update page_directory to map the new RID to its location
        self.page_directory[new_rid] = (total_page_ranges - 1, self.page_ranges[-1].current_record_count() - 1)
        
    def update_record(self, primary_key, *columns):
        """
        Updates an existing record identified by the primary key with new column values.
        The method locates the record using the table's index, then updates the record's values.
        
        Parameters:
        - primary_key: The value of the primary key for the record to be updated.
        - columns (tuple): The new values for the record, corresponding to each column in the table.
        """
        if not self.record_exists(primary_key):
            print("Record with key {} does not exist.".format(primary_key))
            return
        
        rid = self.index.locate(self.key, primary_key)
        
        for record in self.data:
            if record.rid == rid:
                record.columns = columns
                return
        print("Record with key {} not found in data list.".format(primary_key))

    def select_records(self, search_key, search_key_column, query_columns):
        """
        Selects records based on a search key and returns the values of specified columns for those records.
        
        Parameters:
        - search_key: The value to search for in the search_key_column.
        - search_key_column (int): The index of the column to search in.
        - query_columns (list of int): The indices of the columns whose values should be returned for the matching records.
        
        Returns:
        A list of tuples, each containing the values for the specified query_columns from each matching record.
        """
        results = []
        # Step 1: Locate RIDs associated with the search key using the index
        rids = self.index.locate(search_key_column, search_key)
        if not rids:
            print(f"No records found with search key {search_key} in column {search_key_column}.")
            return results

        # Step 2: Iterate over each RID to fetch the record's data
        for rid in rids:
            if rid not in self.page_directory:
                continue  
            
            
            
            # Find the page range and record location using the page directory
            page_range_num, record_location = self.page_directory[rid]
            if page_range_num >= len(self.page_ranges):
                print(f"Invalid page range reference for RID {rid}.")
                continue

            page_range = self.page_ranges[page_range_num]

            # Step 3: Retrieve the record data from the page rang
            record_data = page_range.readRecord(record_location, query_columns) # this needs to change
            if record_data:
                results.append(record_data)
        
        return results

    
    def read(self, rid, query_columns):
        """
        Reads and returns the values of specified columns for a record identified by its RID.
        
        Parameters:
        - rid (int): The record ID of the record to read.
        - query_columns (list of int): The indices of the columns to read.
        
        Returns:
        A list containing the values for the specified query_columns from the record.
        """
        # use page directory to find pageRange for this rid and call it's read function

        # break up RID
        pagerange_num, (basepage_num, offset) = self.page_directory[rid]

        page_range = self.pagerange_array[pagerange_num]

        # Get values for specified columns in query_columns
        record_columns = page_range.readRecord(basepage_num, offset, query_columns)

        selected_record = Record(rid, record_columns[self.key], record_columns)
        return [selected_record]
    
    def delete_record(self, key):
        pass
    
    def record_exists(self, key): 
        pass
    def is_locked(self, key):
        pass

    def generate_rid(self):
        """
        Generates a new unique record ID (RID) for a new record.
        
        Returns:
        An integer representing the new RID.
        """
        self.last_rid += 1
        return self.last_rid
    
    def __merge(self):
        print("merge is happening")
        pass
 
