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

    def delete(self, primary_key):
        """
        Deletes a record identified by the primary key. Checks for record existence and lock status before deletion.

        Parameters:
            primary_key: The primary key of the record to be deleted.

        Returns:
            bool: True if the deletion was successful, False if the record does not exist or is locked.
        """
        self.table.delete_record(primary_key)
        return True    
    
    def insert(self, *columns):
        """
        Inserts a new record into the table with the specified column values.

        Parameters:
            *columns: A variable number of arguments representing the values for each column in the new record.

        Returns:
            bool: True if the record was successfully inserted, False otherwise (e.g., mismatch in column count).
        """
        # try:
        if len(columns) != self.table.num_columns:
            print("Error: Number of values does not match the number columns.")
            return False
        self.table.insert_record(*columns)

        # print("Data inserted successfully!")
        return True
        
        # except Exception as e:
        #     print(f"Error inserting data: {e}")
        #     return False

    
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
        return self.select(search_key, search_key_column, projected_columns_index)
        

    # The following method is marked as "DOES NOT WORK" and needs clarification or debugging:
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # :param primary_key: the value you want to search based on (same as search_key in select)
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        """
        Parameters:
            primary_key: The primary key of the record to update.
            *columns: New values for the record, specified as a sequence of column values. Use None for unchanged columns.

        Returns:
            bool: True if the update was successful, False if no record with the given key exists or due to locking issues.
        """
        
        self.table.update_record(primary_key, *columns)
        

    
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
        # print(start_range, end_range)
        projected_columns_index = [None] * self.table.num_columns
        projected_columns_index[aggregate_column_index] = 1
        for key in range(start_range, end_range + 1):
            # rid = self.table.index.locate(self.table.key, key)
            # if rid is not None:
            record_result = self.table.select_records(key, self.table.key, projected_columns_index)
            if record_result == None:
                continue
            # print("record_result", record_result[0].columns[0])
            total += record_result[0].columns[0]
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
        return self.sum(start_range, end_range, aggregate_column_index)
    
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
