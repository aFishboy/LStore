import json
from lstore.index import Index
import os
from lstore.table import Table

class Database():

    def __init__(self):
        self.path = None
        self.tables = []
        self.num_tables = 0

    def open(self, path):
        if os.path.exists(path):
            self.prev_path = os.getcwd()
            os.chdir(path)
            if os.path.exists('database.json'):
                with open('database.json', 'r') as file:
                    self.tables = json.load(file)
        else:
            print(path, " not found")
            pass

    def close(self):
        for table in self.tables:
            table.close()

        with open('database.json', 'w') as f:
            json.dump(self.tables, f, indent=4)


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
        table = Table(name, num_columns, key_index)
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