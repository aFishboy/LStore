from ..db import Database
from ..query import Query
from random import choice, randint, sample, seed
import pytest

class TestUpdate:
    def test_update(self):
        db = Database()
        # Create a table  with 5 columns
        #   Student Id and 4 grades
        #   The first argument is name of the table
        #   The second argument is the number of columns
        #   The third argument is determining the which columns will be primary key
        #       Here the first column would be student id and primary key
        grades_table = db.create_table('Grades', 5, 0)
        query = Query(grades_table)

        key = 5
        recordIns = [key, 10, 15, 20, 25]
        query.insert(*recordIns)

        recordSel = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        print(recordSel.columns)
        assert recordIns == recordSel.columns
        
        updated_columns = [None, 60, 75, 80, 95]
        # value = 12
        # updated_columns[2] = value
        query.update(key, *updated_columns)
        print("update!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]

        assert record.columns == updated_columns