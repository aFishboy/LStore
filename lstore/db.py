from lstore.buffer_pool import BufferPool
from lstore.index import Index
import os
from lstore.table import Table
from .config import *

class Database():

    def __init__(self):
        self.path = None
        self.tables = []
        self.num_tables = 0
        self.bufferpool = None

    def open(self, path):
        if os.path.exists(path):
            self.prev_path = os.getcwd()
            os.chdir(path)
            if os.path.exists('database.json'):
                with open('database.txt', 'r') as file:
                    pass
                    
        else:
            print(path, " not found")
            pass

    def close(self):
        filename = 'database_info.txt'
        with open(filename, 'w') as f:
            f.write("aaaaaaaaaaaaaaaaaaaaaaaaajhsflkjshdflksjdfhlskdjfhsdlkfjhtest")
            for table in self.tables:
                f.write(f"Table Name: {table.name}\n")
                f.write(f"Number of Columns: {table.num_columns}\n")
                f.write(f"Key Index: {table.key_index}\n")
                f.write(f"Number of Pages: {table.num_pages}\n")
                f.write(f"Page Index: {table.page_index}\n\n")
        for table in self.tables:
            table.close()

        


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    # Create table may be done makes the table and adds it to self.tables and returns table when done
    def create_table(self, name, num_columns, key_index):
        for table in self.tables:
            if name == table.name:
                print(f"Cannot create table with name {name} \n" +
                       "A table with that name already exists")
                return
        if self.bufferpool is None:
            self.bufferpool = BufferPool(BUFFERPOOL_SIZE, "")
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables.append(table)
        self.num_tables += 1
        return table

    
    """
    # Deletes the specified table
    """
    # searches through self.tables to find the specified table
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                break

    
    """
    # Returns table with the passed name
    """
    # searches through self.tables to find the specified table
    
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None # should probably have some error handling added later here or at the function calling it