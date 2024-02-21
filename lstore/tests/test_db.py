import pytest
from ..db import Database
from ..config import *

class TestPage:

    # can create a new Page object
    def test_create_duplicate_table_names(self):
        db = Database()
        grades_table = db.create_table('Grades', 5, 0)
        grades2_table = db.create_table('Grades', 5, 0)
        assert db.num_tables == 1
