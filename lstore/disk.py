from .physical_page import PhysicalPage
import zlib
import os
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
        return PhysicalPage(bytearray(uncompressed_data))
    
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
