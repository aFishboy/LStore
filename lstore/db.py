import csv
import struct
from lstore.buffer_pool import BufferPool
from lstore.index import Index
import os
from lstore.rid_generator import RidGen
from lstore.table import Table
from .config import *
from lstore.disk import Disk

class Database():

    def __init__(self):
        self.path = None
        self.tables = []
        self.current_table_metadata = None
        self.num_tables = 0
        self.bufferpool = None
        self.base_path = None
        self.disk = None
        self.rid_gen = None
        
    def open(self, path):
        self.path = path
        self.disk = Disk(path)
        if not os.path.exists(path):
            os.makedirs(path)  # Create the directory if it doesn't exist
        self.base_path = os.getcwd()
        os.chdir(path)

        self.rid_gen = RidGen(self.disk)
        files = os.listdir(os.getcwd())  # Get a list of all files and directories in the current directory
        for file_to_open in files:
            if file_to_open == "data_base_rid_data.txt" or file_to_open.startswith("page_range"):
                continue
            with open(file_to_open, 'r') as opened_file:
                first_row = opened_file.readline().strip()
                if not first_row:  # Check if the first line is empty
                    raise ValueError(f"The first line of the file '{file_to_open}' is empty.")
                else:
                    elements = first_row.split(';')
                    if len(elements) != 5:
                        raise ValueError(f"Invalid format in the first line of the file '{file_to_open}'.")
                    table_name, num_columns, key_index, total_page_ranges, last_page_range = elements
                    print("metadata",table_name, num_columns, key_index, total_page_ranges, last_page_range)
                    # page_range_bitmap
                    # open_page_ranges_bitmap_list = eval(page_range_bitmap)
                    self.tables.append(Table(table_name, int(num_columns), int(key_index), int(total_page_ranges), self.rid_gen, last_page_range, self.path))
                    self.tables[-1].read_index(file_to_open, self.disk) 
                    self.tables[-1].read_page_directory(file_to_open, self.disk)


                    # prob pass in opened_file^^^^^^^^ instead of reader ^^^^^^^^^^^^^^^^^^^^^^^^

        
                


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
        # with open("db.csv", 'wb') as file:
        #     for i in range(32):  # Ensure 32 table names are written
        #         if i < len(self.table_names):
        #             table_name = self.table_names[i]
        #         else:
        #             table_name = ''  # Use an empty string for empty table names
                
        #         # Pad the table name to 32 bytes
        #         padded_name = table_name.ljust(32, '\0')
        #         # Convert the padded name to bytes and write it to the file
        #         file.write(padded_name.encode('utf-8'))
            
        # Ensure the buffer pool also properly flushes any remaining dirty pages and closes any open files.
        if self.bufferpool is not None:
            # self.bufferpool.close()
            self.bufferpool = None
        
        self.rid_gen.store_rid_data(self.disk)
        for table in self.tables:
            self.disk.write_table_metadata(table)
            table.evict_bufferpool()
            
        

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    # Create table may be done makes the table and adds it to self.tables and returns table when done
    def create_table(self, name, num_columns, key_index):

        table_file_name = name + ".txt"
        if os.path.exists(table_file_name):
            print("Table file already exists: {}".format(table_file_name))
            raise FileExistsError("Table file already exists: {}".format(table_file_name)) 
        else:
            # File does not exist, create it with default data or an empty state
            print("Creating table_file:", table_file_name)
            with open(table_file_name, 'w') as file:
                pass
            self.tables.append(Table(table_file_name, num_columns, key_index, 0, self.rid_gen, -1, self.path))
            print("table_file created.")
        self.tables[-1].read_index(None, self.disk) 
        self.tables[-1].read_page_directory(None, self.disk) 
        return self.tables[-1]

    
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
        adjusted_name = name + ".txt"
        for table in self.tables:
            if table.name == adjusted_name:
                return table
        return None # should probably have some error handling added later here or at the function calling it