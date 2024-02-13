from lstore.index import Index
from time import time
from page_range import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.data = []
        self.index = Index(self)
        self.page_ranges = [] 
        self.last_rid = -1
        self.page_directory = {} # maps RID to tuple, (What page_range, what record location in page) 
        pass

    def insert_record(self, *columns):
        total_page_ranges = len(self.page_ranges)
        new_rid = self.generate_rid()

        # if total_page_ranges == 0 or not self.page_ranges[-1].hasCapacity():   prob can invert this and save lines

        if  total_page_ranges != 0 and self.page_ranges[-1].hasCapacity():
            self.page_ranges[-1].addNewRecord(new_rid, *columns)
        else:
            self.page_ranges.append(PageRange(self.num_columns))
            total_page_ranges += 1
            self.page_ranges[-1].addNewRecord(new_rid, *columns)

        self.page_directory[new_rid] = (total_page_ranges - 1, len(self.page_ranges[-1].base_pages) - 1) #location of record. What page range and which page block in that range
        

    def update_record(self, primary_key, *columns):
        if not self.record_exists(primary_key):
            print("Record with key {} does not exist.".format(primary_key))
            return
        
        rid = self.index.locate(self.key, primary_key)
        
        for record in self.data:
            if record.rid == rid:
                record.columns = columns
                return
        print("Record with key {} not found in data list.".format(primary_key))


    def select_records(self, search_key, search_key_column, projected_columns_index):
        for record in self.data:
            if record.rid == rid:
                result = [record.columns[i] for i in query_columns]
                return result
        print("Record with RID {} not found in data list.".format(rid))
        return None

    def delete_record(self, key):
          # Check if the record exists
    if not self.record_exists(key):
        print("Record with key {} does not exist.".format(key))
        return
    # Locate the RID of the record to delete
    rid = self.index.locate(self.key, key)
    # Find and remove the record from the data list
    for i, record in enumerate(self.data):
        if record.rid == rid:
            del self.data[i]
            break
    # Remove the record from the index
    self.index.remove(key)
    print("Record with key {} deleted successfully.".format(key))
        pass
    
    def record_exists(self, key): 
           # Check if the record exists in the index
    return self.index.locate(self.key, key) != -1
        pass

    def is_locked(self, key):
          locked_records = {
        1: True,  # Example of a locked record with key 1
        2: False  # Example of an unlocked record with key 2
        # Add more records as needed
    }
    if key in locked_records:
        return locked_records[key]
    else:
        return False  # Default assumption: record is not locked if not found in the dictionary
        pass

    def generate_rid(self):
        self.last_rid += 1
        return self.last_rid
    
    def __merge(self):
        print("merge is happening")
        pass
 
