import csv
import linecache
import re
import zlib
import os
from lstore.page import Page
from lstore.table import Table
from avltree import AvlTree



class Disk:
    def __init__(self, path) -> None:
        if len(path) > 0 and path[-1] == "/":
            self.path = path[:-1]
        else:
            self.path = path
    
    def get_page(self, page_id):
        file_name = self.make_file_name(page_id)
        while True:
            with open(file_name, "rb") as file:
                compressed_data = file.read()
            if len(compressed_data) != 0:
                break
        uncompressed_data = zlib.decompress(compressed_data)
        return Page(bytearray(uncompressed_data))
    
    def write_page(self, page_id, page_to_write):
        file_name = self.make_file_name(page_id)
        data = page_to_write.get_data()
        compressed_data = zlib.compress(data)
        with open(file_name, "wb") as file:
            file.write(compressed_data)
        
    def page_exists(self, page_id):
        file_name = self.make_file_name(page_id)
        return os.path.isfile(file_name)
    
    def make_file_name(self, page_id):
        file_name_to_return = self.path + "/" + page_id
        return file_name_to_return
    
    def write_table_metadata(self, table):
        table_file_name = table.name
        print("disk table name", table_file_name)
        num_columns = table.num_columns 
        key_index = table.key
        file_metadata = f"{table_file_name},{num_columns},{key_index}".ljust(50)
        file_metadata = file_metadata + '\n' + str(table.index)
        file_metadata = file_metadata + '\n' + str(table.page_directory)

        with open(table_file_name, 'r+') as file:
            # Move the file pointer to the beginning
            file.seek(0)
            buffer = file.read(50)
            file.seek(0)
            # Write the modified first line
            file.write(file_metadata)
            # Truncate the remaining content (in case the new line is shorter than the old one)
    
    def get_rid_data(self):
        with open("data_base_rid_data.txt", 'a+', newline='') as file:
            reader = csv.reader(file)
            first_row = next(reader, [])  # Get the first row or an empty list if file is empty
            if not first_row:  # Check if the first row is empty
                return -1, 0
            else:
                # Extract the values from the first row
                last_base_rid, last_tail_rid = map(int, first_row)
                return last_base_rid, last_tail_rid

    def store_rid_data(self, last_base_rid, last_tail_rid):
        with open("data_base_rid_data.txt", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([last_base_rid, last_tail_rid])
    
    def read_index(self, file_name, num_columns):
        line = linecache.getline(file_name, 2) # NOT ZERO INDEXED
        if line == "":
            print("made it")
            list_of_avls = [AvlTree() for _ in range(num_columns)]
            return list_of_avls
        list_of_strings = self.extract_data_from_string(line)
        list_of_dicts = []
        for string in list_of_strings:
            list_of_dicts.append(eval(string))
        list_of_avls = []
        for dict in list_of_dicts:
            list_of_avls.append(AvlTree(dict))
        return list_of_avls
    
    def read_page_directory(self, file_name):
        line = linecache.getline(file_name, 3) # NOT ZERO INDEXED
        return line
        
    
    def extract_data_from_string(self, string):
        pattern = r'AvlTree\((.*?)\)'  # Define the regular expression pattern to match the AvlTree data
        data_list = re.findall(pattern, string)  # Find all matches of the pattern in the string
        return data_list


