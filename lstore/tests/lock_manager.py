#locks a table
from lstore.buffer_pool import BufferPool
from lstore.index import Index
from lstore.table import Table, Record
from lstore.index import Index
import os
from lstore.rid_generator import RidGen
from .config import *
from lstore.disk import Disk

class LockManager:
    def __init__(self):
        self.lock_table = {}

    def acquire_lock(self, table, record_id, lock_type):
        if table not in self.lock_table:
            self.lock_table[table] = {}
        if record_id not in self.lock_table[table]:
            self.lock_table[table][record_id] = set()

        if lock_type == LockType.SHARED:
            if self.lock_table[table][record_id] & {LockType.EXCLUSIVE}:
                return False
            else:
                self.lock_table[table][record_id].add(lock_type)
        elif lock_type == LockType.EXCLUSIVE:
            if self.lock_table[table][record_id]:
                return False
            else:
                self.lock_table[table][record_id].add(lock_type)
        return True

    def release_lock(self, table, record_id):
        if table in self.lock_table and record_id in self.lock_table[table]:
            del self.lock_table[table][record_id]
