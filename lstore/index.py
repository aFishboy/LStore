"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from avltree import AvlTree
from .page import Page

class Index:

    def __init__(self, table):
        self.indices = [AvlTree() for _ in range(table.num_columns)]  # Initialize each column's index as an OOBTree
        self.key = table.key
        self.column_num = dict()
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        """
        Returns the location of all records with the given value on column "column".
        """
        # if column != self.key:
        #     print("Lookup on non-primary key columns not supported in this context.")
        #     return None
        column_btree = self.indices[column]
        try:
            value = column_btree[value]
            return value
        except KeyError:
            return None


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
    
    """
    Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
