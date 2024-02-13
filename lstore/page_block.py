from lstore.page import Page

class PageBlock:
    def __init__(self, num_columns) -> None:
        """
        Initializes a new PageBlock instance, which represents a collection of pages,
        each page corresponding to a column in the table.
        
        Parameters:
        - num_columns (int): The number of columns (and thus the number of pages) in the PageBlock.
        """
        self.num_columns = num_columns
        self.column_pages = [Page() for _ in range(self.num_columns)]
        self.records_in_page = 0 # might not need

    def has_capacity(self):
        """
        Checks if the PageBlock has enough capacity to add another record across all column pages.
        
        Returns:
        True if there is capacity for more records in all column pages, False otherwise.
        """
        if self.column_pages[0].has_capacity():
            return True
        return False
    
    def write(self, *columns):
        """
        Writes a new record to the PageBlock, distributing the column values across the appropriate column pages.
        
        Parameters:
        - columns (tuple): The column values of the new record.
        """
        # add the record values to pages
        for i in range(self.num_columns):
            self.column_pages[i].write(columns[i])
            #self.column_pages[i].increment_record_count() # may need to be on the outside of for loop and only happen once idk