import csv
import zlib
import os
from lstore.page import Page

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
    
    def write_table_metadata(self, table_file_name, num_columns, key_index):
        with open(table_file_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([table_file_name, num_columns, key_index])
    
    def get_rid_data(self):
        with open("data_base_rid_data.csv", 'a+', newline='') as file:
            reader = csv.reader(file)
            first_row = next(reader, [])  # Get the first row or an empty list if file is empty
            if not first_row:  # Check if the first row is empty
                return -1, 0
            else:
                # Extract the values from the first row
                last_base_rid, last_tail_rid = map(int, first_row)
                return last_base_rid, last_tail_rid

    def store_rid_data(self, last_base_rid, last_tail_rid):
        with open("data_base_rid_data.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([last_base_rid, last_tail_rid])

