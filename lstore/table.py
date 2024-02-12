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


    def read_record(self, rid, query_columns):
        for record in self.data:
            if record.rid == rid:
                result = [record.columns[i] for i in query_columns]
                return result
        print("Record with RID {} not found in data list.".format(rid))
        return None

    def delete_record(self, key):
        pass
    
    def record_exists(self, key):
        pass
    def is_locked(self, key):
        pass

    def generate_rid(self):
        self.last_rid += 1
        return self.last_rid
    
    def __merge(self):
        print("merge is happening")
        pass
 
