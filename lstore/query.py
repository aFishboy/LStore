from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    The Query class provides methods to perform CRUD operations and queries on a specified table.
    It supports insertion, deletion, selection, updates, and aggregations of records within the table.
    """

    def __init__(self, table):
        """
        Initializes the Query object with a reference to the table on which operations will be performed.

        Parameters:
            table (Table): The table object on which the query operations are to be executed.
        """
        self.table = table
        pass


    def delete(self, primary_key):
        """
        Deletes a record identified by the primary key. Checks for record existence and lock status before deletion.

        Parameters:
            primary_key: The primary key of the record to be deleted.

        Returns:
            bool: True if the deletion was successful, False if the record does not exist or is locked.
        """
        if not self.record_exists(primary_key):
            return False
        if self.is_locked(primary_key): #need to add is_locked function
            return False
        self.delete_record(primary_key)
        return True    
    
    def insert(self, *columns):
        """
        Inserts a new record into the table with the specified column values.

        Parameters:
            *columns: A variable number of arguments representing the values for each column in the new record.

        Returns:
            bool: True if the record was successfully inserted, False otherwise (e.g., mismatch in column count).
        """
        schema_encoding = '0' * self.table.num_columns # What is this for
        try:
            if len(columns) != self.table.num_columns:
                print("Error: Number of values does not match the number columns.")
                return False
            self.table.insert_record(*columns)

            #print("Data inserted successfully!")
            return True
        
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_column: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    # search key is what we are looking for for example a name
    # search key index is the column it would be in for example if it is a name it would be in the name column 
    # projected_columns_index is what columns we want to return of the record/s if / when we find i think
    def select(self, search_key, search_key_column, projected_columns_index):
        """
        Selects records matching a search key from a specified column and returns only specified columns.

        Parameters:
            search_key: The value to search for in the specified column.
            search_key_column (int): The index of the column to search.
            projected_columns_index (list[int]): A list indicating which columns to include in the result (1=include, 0=exclude).

        Returns:
            list[Record]: A list of Record objects matching the search criteria, False if a record lock prevents selection.
        """
        found_matching_records = []
        found_matching_records = self.table.select_records(search_key, search_key_column, projected_columns_index)
        return found_matching_records
    
        for i in range(len(projected_columns_index)):
            if projected_columns_index[i] == 1:
                arr.append(i)
        projected_columns_index = arr
        baseRecord_RID = self.table.index.locate(search_key_column, search_key)
        #selected_record = self.table.read(baseRecord_RID, projected_columns_index)
        # need to fix table.read, add it
        selected_record = {}
        return selected_record
        
   

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_column: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_column, projected_columns_index, relative_version):
        selected_records = []

        for record in self.records:
            if record[search_key_column] == search_key:
                if len(record) > relative_version:
                    selected_record = [record[i] for i in range(len(record)) if projected_columns_index[i] == 1]
                    selected_records.append(selected_record)
                else:
                    # Handle the case when relative_version exceeds the record length
                    return False

        return selected_records
        

    # The following method is marked as "DOES NOT WORK" and needs clarification or debugging:
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # :param primary_key: the value you want to search based on (same as search_key in select)
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        """
        Updates the record identified by the primary key with new values for specified columns.
        baseRecord_RID = self.table.index.locate(primary_key)
        query_columns = [i for i in range(len(columns))]
        selected_record = self.table.read(baseRecord_RID,query_columns)
        selected_record = {}

        Parameters:
            primary_key: The primary key of the record to update.
            *columns: New values for the record, specified as a sequence of column values. Use None for unchanged columns.

        Returns:
            bool: True if the update was successful, False if no record with the given key exists or due to locking issues.

        Note: This function is marked as "DOES NOT WORK" because it lacks mechanisms to handle locks or validate primary key existence.
        """
        self.table.update_record(primary_key, *columns)
        # # Locate the base record's RID using the primary key.
        # base_rid = self.table.index.locate(self.table.key, primary_key)
        # if base_rid is None:
        #     print(f"No record found with primary key: {primary_key}")
        #     return False
        # # Ensure base_rid is a singular value.
        # if isinstance(base_rid, list):
        #     if len(base_rid) == 1:
        #         base_rid = base_rid[0]
        #     else:
        #         print(f"Multiple or no records found with primary key: {primary_key}, base_rid: {base_rid}")
        #         return False
        # elif base_rid is None:
        #     print(f"No record found with primary key: {primary_key}")
        #     return False

        # # Proceed with the update using base_rid as a singular RID.
        # updated_columns = [None if col is None else col for col in columns]
        # success = self.table.add_tail_record(base_rid, updated_columns)
        # return success

    
    # DOES NOT WORK
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        for key in range(start_range, end_range + 1):
            rid = self.table.index.locate(self.table.key, key)
            if rid is not None:
                total += self.table.read_record(rid, [aggregate_column_index])[0].columns[0]
        return total


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        summation = 0
        for record_index in range(start_range, end_range + 1):
            try:
                value = self.read_version(record_index, relative_version, aggregate_column_index)
                summation += value 
            except IndexError:
                continue
        
        if summation == 0:
            return False 
        else:
            return summation

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
