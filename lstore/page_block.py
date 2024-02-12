from page import Page

class PageBlock:
    def __init__(self, num_columns) -> None:
        self.num_columns = num_columns
        self.column_pages = [Page() for _ in range(self.num_columns)]
        self.records_in_page = 0 # might not need

    def has_capacity(self):
        if self.column_pages[0].has_capacity():
            return True
        return False
    
    def write(self, *columns):
        # add the record values to pages
        for i in range(self.num_columns):
            self.column_pages[i].write(columns[i])
            #self.column_pages[i].increment_record_count() # may need to be on the outside of for loop and only happen once idk