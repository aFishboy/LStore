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
        pass

    def insert_record(self, *columns):
        total_page_ranges = len(self.page_ranges)
        new_rid = self.generate_rid()

        if  total_page_ranges != 0 and self.page_ranges[-1].hasCapacity():
            self.page_ranges[-1].addNewRecord(new_rid, *columns)
        else:
            self.page_ranges.append(PageRange(self, self.num_columns))
            total_page_ranges += 1
         

    def generate_rid(self):
        self.last_rid += 1
        return self.last_rid
    
    def __merge(self):
        print("merge is happening")
        pass
 
