from page import Page

class PageRange:
    def __init__(self, table, num_columns) -> None:
        self.table = table
        self.num_columns = num_columns
        self.pages_per_range = 16 # 8192 records per range
        self.base_pages = []
        self.tail_pages = []
        self.num_records = 0
        self.last_base_offset = 0
        self.last_tail_offset = 0
        self.last_base_page = 0
        self.last_tail_page = 0        
        # Create base pages with RID, indirection, and schema encoding column
        self.base_pages.append([Page() for _ in range(self.num_columns + 3)]) 
        # Create tail pages with RID and indirection
        self.tail_pages.append([Page() for _ in range(self.num_columns + 2)]) 

    def addNewRecord(self, rid, *columns):
        self.num_records += 1
        if not self.base_pages[-1][0].has_capacity():
            # last base page full; create new base page
            self.base_pages.append([Page() for _ in range(self.num_columns + 3)])
            self.last_base_offset = 0
            self.last_base_page += 1

        page = self.base_pages[self.last_base_page]
        # add the record values to pages
        for i in range(self.num_columns):
            page[i].write(columns[i], self.last_base_offset)
        page.increment_record_count()
        # add RID
        page[self.num_columns].write(rid, self.last_base_offset)
        # add indirection
        page[self.num_columns + 1].write(0, self.last_base_offset)
        # add schema encoding
        page[self.num_columns + 2].write(0, self.last_base_offset)

        self.last_base_offset += 8
        return (self.last_base_page, self.last_base_offset - 8)
