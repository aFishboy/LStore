"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree
from page import Page

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None for _ in range(table.num_columns)]
        self.key_column = table.key_column
        self.column_num = dict()
        self.table = table
        
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        column_btree = self.indices[column]
        if not column_btree.has_key(value):
            return []
        return column_btree[value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return_list = []
        column_btree = self.indices[column]
        for list1 in list(column_btree.values(min=begin, max=end)):
            return_list += list1
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
