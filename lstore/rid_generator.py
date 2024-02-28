import csv
import zlib
import os
from lstore.page import Page

class RidGen():
    def __init__(self, disk):
        self.last_base_rid, self.last_tail_rid = disk.get_rid_data()
        # print(self.last_base_rid, self.last_tail_rid)
    
    def generate_base_rid(self):
        self.last_base_rid += 1
        return self.last_base_rid

    def generate_tail_rid(self):
        self.last_tail_rid += 1
        return self.last_tail_rid
    
    def store_rid_data(self, disk):
        disk.store_rid_data(self.last_base_rid, self.last_tail_rid)


    

    