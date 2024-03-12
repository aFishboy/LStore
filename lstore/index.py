"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from avltree import AvlTree
from .page import Page

class Index:

    def __init__(self, table, list_of_AvlTrees):
        self.indices = [list_of_AvlTrees[i] for i in range(table.num_columns)]  # Initialize each column's index as an OOBTree
        self.key = table.key
        self.column_num = dict()
        self.table = table
    
    def __str__(self):
        return ''.join(str(avl_tree) for avl_tree in self.indices)

    """
    # returns the location of all records with the given value on column "column"
    """

    def create_index(self, index_column_to_create):
        self.table.create_index(index_column_to_create)

    def locate(self, column, value):
        """
        Returns the location of all records with the given value on column "column".
        """
        # if column != self.key:
        #     print("Lookup on non-primary key columns not supported in this context.")
        #     return None
        avl_tree = self.indices[column]
        try:
            value = avl_tree[value]
            return value
        except KeyError:
            print("key error")
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


    def get_value_and_location(self, rec_ind, base_page, column_number):
        """
        Determines the value to index and its location for a given record.
        
        Args:
            rec_ind (int): The record index in the base page.
            base_page: The base page object.
            column_number (int): The column number to index.
            
        Returns:
            tuple: (value, location) if the record should be indexed, else (None, None).
        """
        tail = self.get_tail_page(rec_ind, base_page)
        base_schema = base_page.schema[column_number]  # NEED TO FIX
        
        if tail is None or base_schema[column_number] == 0:
            # Use value from the base page if no tail record or schema indicates base record should be used
            if not self.rec_is_deleted(rec_ind, base_page):
                value = base_page.records[column_number][rec_ind]  # NEED TO FIX
                return value, base_page.path  # Adjust path retrieval based on your structure
        else:
            # Use value from the tail record
            tail_page, tail_rec_ind = tail
            value = tail_page.records[column_number][tail_rec_ind]  # NEED TO FIX
            return value, base_page.path  # Use base page path for location
        
        return None, None  # Return None if the record shouldn't be indexed
    
    def get_base_paths(self):
        """
        Retrieves the paths of all base pages in the table.
        
        Returns:
            list: The list of paths for all base pages.
        """
        pass  
    
    def get_tail_page(self, rec_ind, base_page):
        """
        Retrieves the tail page and record index for a given base record.
        
        Args:
            rec_ind (int): The record index in the base page.
            base_page: The base page object.
            
        Returns:
            tuple: (tail_page, tail_rec_ind) if a tail record exists, else None.
        """
        pass
    
    def get_page(self, path):
        """
        Retrieves the page object for a given path. Needs to check the buffer
        
        Args:
            path (str): The path to the page.
            
        Returns:
            Page: The page object.
        """
        pass
    
    def rec_is_deleted(self, rec_ind, base_page):
        """
        Determines if a record is deleted.
        
        Args:
            rec_ind (int): The record index in the base page.
            base_page: The base page object.
            
        Returns:
            bool: True if the record is deleted, else False.
        """
        pass
    
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, index_column_to_drop):
        self.table.drop_index(index_column_to_drop)

