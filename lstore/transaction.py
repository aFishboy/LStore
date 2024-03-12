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
        self.lock_manager = lock_manager
        

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            if not self.acquire_locks(table, args):
                return self.abort()
            result = query(*args)
           
            # Release locks after executing the query
            self.release_locks(table, args)
             # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()
    
    def acquire_locks(self, table, args):
        for record_id in args:
            if not self.lock_manager.acquire_lock(table, record_id, LockType.EXCLUSIVE):
                return False
        return True

    def release_locks(self, table, args):
        for record_id in args:
            self.lock_manager.release_lock(table, record_id)

    
    def abort(self):
        for query, table, args in reversed(self.queries):
            query.rollback(*args)
        
        self.release_locks()
        return False

    
    def commit(self):
        for query, table, args in self.queries:
            query.finalize(*args)
        
        self.release_locks()
        return True

