import base64
import csv
import linecache
import re
import zlib
import os
from lstore.page import Page
from lstore.page_range import PageRange
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

        # Write the metadata and serialized data to the file
        table_file_name = table.name
        with open(table_file_name, 'w', encoding='utf-8') as file:
            # Write metadata to the first line
            file.write(f"{table_file_name};{table.num_columns};{table.key};{table.total_page_ranges};{table.last_page_range}\n")
            file.write(str(table.index) + '\n')
            file.write(str(table.page_directory) + '\n\n')
    
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
        print(file_name,"line", line)
        return line
        
    
    def extract_data_from_string(self, string):
        pattern = r'AvlTree\((.*?)\)'  # Define the regular expression pattern to match the AvlTree data
        data_list = re.findall(pattern, string)  # Find all matches of the pattern in the string
        return data_list


