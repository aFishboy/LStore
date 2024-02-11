from page import Page

class PageRange:
    def __init__(self, table, num_columns) -> None:
        self.table = table
        self.num_columns = num_columns
        self.pages_per_range = 16 # 8192 records per range
        self.num_records = 0
        self.last_tail_offset = 0
        self.last_base_page = 0
        self.last_tail_page = 0   
        self.base_pages = []
        self.tail_pages = []     
        # Create base pages with RID, indirection, and schema encoding column
        self.base_pages.append([Page() for _ in range(self.num_columns + 3)]) 
        # Create tail pages with RID and indirection
        self.tail_pages.append([Page() for _ in range(self.num_columns + 2)]) 

    def addNewRecord(self, rid, *columns):
        self.num_records += 1
        if not self.base_pages[-1][0].has_capacity():
            # last base page full; create new base page
            self.base_pages.append([Page() for _ in range(self.num_columns + 3)])
            self.last_base_page += 1

        page = self.base_pages[self.last_base_page]
        # add the record values to pages
        for i in range(self.num_columns):
            page[i].write(columns[i])
            page[i].increment_record_count() # may need to be on the outside of for loop and only happen once idk
        # add RID
        page[self.num_columns].write(rid)
        page[self.num_columns].increment_record_count()
        # add indirection
        page[self.num_columns + 1].write(0)
        page[self.num_columns + 1].increment_record_count()
        # add schema encoding
        page[self.num_columns + 2].write(0)
        page[self.num_columns + 2].increment_record_count() # need to find better way to do this

