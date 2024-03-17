import time
from lstore.table import Table, Record
from lstore.index import Index
from enum import Enum

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        # self.lock_manager = lock_manager
        self.successful_queries = 0
        self.table = None
        

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table == None:
            self.table = table

        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            if not self.acquire_locks(self.table):
                # print("failed!!!!!!!!!!!!!!!!!")
                return self.abort()
            # print("grabbed!!!!!!!!!!!!!!!!")

            # if not self.acquire_locks(table, args):
            #     return self.abort()
            result = query(*args)
           
            # Release locks after executing the query
             # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            self.successful_queries += 1
            self.release_locks(self.table)
        return self.commit()
    
    def acquire_locks(self, table):
        return table.acquire_lock()

    def release_locks(self, table):
        table.release_lock()

    
    def abort(self):
        # for query, table, args in reversed(self.queries):
        #     query.rollback(*args)
        
        self.release_locks(self.table)
        time.sleep(0)
        return False

    
    def commit(self):
        # for query, table, args in self.queries:
        #     query.finalize(*args)
        return True
        
        self.release_locks()
        return True

