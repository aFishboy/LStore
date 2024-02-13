from page import Page
from page_block import PageBlock

class PageRange:
    def __init__(self, num_columns) -> None:
        self.num_columns = num_columns
        self.base_pages_per_range = 8
        self.num_records = 0
        self.base_pages = []
        self.tail_pages = []     
        # Create base pages with RID, indirection, and schema encoding column
        self.base_pages.append(PageBlock(self.num_columns + 3)) 
        # Create tail pages with RID and indirection

    def addNewRecord(self, rid, *columns):
        self.num_records += 1
        if not self.base_pages[-1].has_capacity():
            # last base page full; create new base page
            self.base_pages.append(PageBlock(self.num_columns + 3))

        base_page_to_write = self.base_pages[-1]
        
        # Group columns with additional values
        data_to_write = list(columns)  # Convert columns tuple to a list
        data_to_write.extend([rid, 0, 0])  # Append additional values
        
        # Write the data to the base page
        base_page_to_write.write(*data_to_write)

    def has_capacity(self):
        if len(self.base_pages) < self.base_pages_per_range or self.base_pages[-1].has_capacity():
            return True
        return False