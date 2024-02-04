class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096) # 4096 bytes per page
        self.RECORD_SIZE = 8  # 8 bytes for a 64-bit integer
        self.MAX_RECORDS_PER_PAGE = len(self.data) // RECORD_SIZE # 4096 / 8 = 512

    def has_capacity(self):
        return self.num_records < self.MAX_RECORDS_PER_PAGE

# Need to do
    def write(self, value):
        self.num_records += 1
        pass

#Prob need a read function