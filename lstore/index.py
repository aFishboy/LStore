"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree
from lstore.page import Page

class Index:
    def __init__(self, table):
        self.indices = [OOBTree() for _ in range(table.num_columns)]  # Initialize each column's index as an OOBTree
        self.key = table.key
        self.column_num = dict()
        self.table = table

    def locate(self, column, value):
        """
        Returns the location of all records with the given value on column "column".
        """
        column_btree = self.indices[column]
        if value not in column_btree:  # Use 'in' to check for a key's existence
            return []
        return column_btree[value]

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end".
        """
        return_list = []
        column_btree = self.indices[column]
        # Use values(min, max) to retrieve a range of keys
        for record_list in column_btree.values(min=begin, max=end):
            return_list.extend(record_list)
        return return_list
    
    def locate(self, column, value):
        column_btree = self.indices[column]
        if not column_btree.has_key(value):
            return []
        return column_btree[value]
    
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
