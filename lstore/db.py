import struct
from lstore.buffer_pool import BufferPool
from lstore.index import Index
import os
from lstore.table import Table
from .config import *

class Database():

    def __init__(self):
        self.path = None
        self.tables = []
        self.table_names = []
        self.num_tables = 0
        self.bufferpool = None
        self.name = None
        
    def open(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)  # Create the directory if it doesn't exist
        
        prev_path = os.getcwd()
        os.chdir(path)
        database_file = "db.txt"
        if os.path.exists(database_file):
            print(" database file exists:", database_file)
            # File exists, read the table names
            self.table_names = self.read_table_names(database_file)
        else:
            # File does not exist, create it with default data or an empty state
            print("Creating database file:", database_file)
            with open(database_file, 'w') as file:
                pass
            self.table_names = []
            print("Database file created.")
        print(self.table_names)

        #     if self.bufferpool is None:
    #         self.bufferpool = BufferPool(BUFFERPOOL_SIZE, path, self.name)

    def read_table_names(self, path):
        table_names = []
        with open(path, 'rb') as file:
            for _ in range(32):  # Read 32 times
                # Read 32 bytes from the file
                data = file.read(32)
                if not data:
                    break
                # Unpack the binary data into a string
                table_name_bytes = struct.unpack('32s', data)[0]
                # Decode the bytes to get the table name
                table_name = table_name_bytes.decode('utf-8').strip('\x00')
                # Append the table name to the list
                table_names.append(table_name)
                print("read table")
        return table_names

    def write_table_name(self, table_name):
        # Encode the table name to bytes
        table_name_bytes = table_name.encode('utf-8')

        # Ensure table name is no longer than 32 bytes
        table_name_bytes = table_name_bytes[:32].ljust(32, b'\x00')

        # Pack the table name into a binary string
        packed_table_name = struct.pack('32s', table_name_bytes)

        # Write the packed table name to the file
        with open(self.path, 'r+b') as file:
            file.seek(0)  # Move to the beginning of the file
            file.write(packed_table_name)
            
    

    def close(self):
        with open("db.txt", 'wb') as file:
            for i in range(32):  # Ensure 32 table names are written
                if i < len(self.table_names):
                    table_name = self.table_names[i]
                else:
                    table_name = ''  # Use an empty string for empty table names
                
                # Pad the table name to 32 bytes
                padded_name = table_name.ljust(32, '\0')
                # Convert the padded name to bytes and write it to the file
                file.write(padded_name.encode('utf-8'))
            
        # Ensure the buffer pool also properly flushes any remaining dirty pages and closes any open files.
        if self.bufferpool is not None:
            # self.bufferpool.close()
            self.bufferpool = None

        


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
            self.bufferpool = BufferPool(BUFFERPOOL_SIZE, self.path, name)
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.name = name
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