from .page import Page
from .config import *

class PageBlock: 
    def __init__(self, num_columns) -> None:
        self.num_columns = num_columns
        self.column_pages = [Page() for _ in range(self.num_columns)]
        self.records_in_page = 0 # might not need
        self.last_written_offset = 0
        self.isFull = False
        self.MAX_RECORDS_PER_PAGE = PAGE_DATA_SIZE // COLUMN_DATA_SIZE # 4096 / 8 = 512
        self.bitmap = [0] * self.MAX_RECORDS_PER_PAGE #move to page block level

    
    def delete(self, offset_to_delete):
        for i in range(self.num_columns):
            self.column_pages[i].delete(offset_to_delete)

    def has_capacity(self):
        if self.column_pages[0].has_capacity():
            return True
        return False
    
    def write(self, *columns):
        # add the record values to pages
        self.last_written_offset = self.column_pages[0].current_offset()
        for i in range(self.num_columns):
            self.column_pages[i].write(columns[i])
            #self.column_pages[i].increment_record_count() # may need to be on the outside of for loop and only happen once idk
    
    def get_record(self, record_index, projected_columns_index):
        record_to_return = []

        for index, column_val in enumerate(projected_columns_index):
                if column_val == 1:    
                    record_to_return.append(self.column_pages[index].read(record_index))
        record_to_return.append(self.column_pages[self.num_columns - 3].read(record_index))
        record_to_return.append(self.column_pages[self.num_columns - 2].read(record_index))
        return record_to_return
    
    # DOES NOT WORK FULLY
    def write_tail_record(self, rid, tail_record):
        for i, value in enumerate(tail_record):
            if value is not None:
                # Check for capacity before writing
                if not self.column_pages[i].has_capacity():
                    raise Exception("Page is full, cannot write tail record")
                self.column_pages[i].write(value)

    def update_base_record_indirection(self, new_rid, record_index):
        # self.print_record(record_index)
        self.column_pages[-2].update_entry(record_index, new_rid)
        # self.print_record(record_index)

    def print_record(self, record_index):
        print("update_base_record_indirection")
        for i in range(self.num_columns):
            print(self.column_pages[i].read(record_index), end=' ')
        print("")
    
