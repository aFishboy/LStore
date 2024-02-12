from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns # What is this for
        try:
            if len(columns) != self.table.num_columns:
                print("Error: Number of values does not match the number columns.")
                return False
            self.table.insert_record(*columns)

            print("Data inserted successfully!")
            return True
        
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    # search key is what we are looking for for example a name
    # search key index is the column it would be in for example if it is a name it would be in the name column 
    # projected_columns_index is what columns we want to return of the record if / when we find i think
    def select(self, search_key, search_key_index, projected_columns_index):
        arr = []
        for i in range(len(projected_columns_index)):
            if projected_columns_index[i] == 1:
                arr.append(i)
        projected_columns_index = arr
        baseRecord_RID = self.table.index.locate(search_key_index, search_key)
        #selected_record = self.table.read(baseRecord_RID, projected_columns_index)
        # need to fix table.read, add it
        selected_record = {}
        return selected_record
        
   

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        selected_records = []

        for record in self.records:
            if record[search_key_index] == search_key:
                if len(record) > relative_version:
                    selected_record = [record[i] for i in range(len(record)) if projected_columns_index[i] == 1]
                    selected_records.append(selected_record)
                else:
                    # Handle the case when relative_version exceeds the record length
                    return False

        return selected_records
        

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # :param primary_key: the value you want to search based on (same as search_key in select)
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        baseRecord_RID = self.table.index.locate(primary_key)
        selected_record = self.table.read(baseRecord_RID)
        selected_record = {}

        try:
            if len(columns) != self.table.num_columns:
                print("Error: Number of values does not match the number columns.")
                return False
           
            self.table.update_record(selected_record)

            print("Data Updated successfully!")
            return True
        
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        try:
            for record in self.table.data:
                # Assuming primary key is the first column
                primary_key = record[0]  
                if start_range <= primary_key <= end_range:
                    total_sum += record[aggregate_column_index]
            return total_sum
        except Exception as e:
            print(f"Error during sum operation: {e}")
            return False

    
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
        pass

    
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
