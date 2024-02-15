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

        records = dict()
        records[5] = [5, 10, 15, 20, 25]
        records[105] = [105, 10, 115, 120, 125]
        records[205] = [205, 210, 215, 220, 225]

        for key in records:
            query.insert(*records[key])

        recordSel = query.select(5, 0, [1, 1, 1, 1, 1])[0]
        print(records[5], "\n", recordSel.columns)
        assert records[5] == recordSel.columns

        recordSel = query.select(105, 0, [1, 1, 1, 1, 1])[0]
        print("\n", records[105], "\n", recordSel.columns)
        assert records[105] == recordSel.columns

        recordSel = query.select(205, 0, [1, 1, 1, 1, 1])[0]
        print("\n", records[205], "\n", recordSel.columns)
        assert records[205] == recordSel.columns
        
        updated_columns = [None, 60, 75, 80, 95]
        query.update(5, *updated_columns)

        updated_columns = [None, 160, 175, 180, 195]
        query.update(5, *updated_columns)

        updated_columns = [None, 333, 444, 555, 666]
        query.update(105, *updated_columns)

        record = query.select(5, 0, [1, 1, 1, 1, 1])[0]

        assert record.columns == [5, 160, 175, 180, 195]