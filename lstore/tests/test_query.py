import pytest
from ..table import Table
from ..query import Query
import pytest

class TestQuery:

    # insert a record with correct number of columns
    def test_insert_record_correct_columns(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        result = query.insert(1, "John", 25)
        assert result == True
        assert len(table.data) == 1
        assert table.data[0] == [1, "John", 25]

    # select a record with existing search key
    def test_select_existing_search_key(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        query.insert(1, "John", 25)
        result = query.select(1, 0, [1, 1, 1])
        assert result == [[1, "John", 25]]

    # delete a record with existing primary key
    def test_delete_existing_primary_key(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        query.insert(1, "John", 25)
        result = query.delete(1)
        assert result == True
        assert len(table.data) == 0

    # insert a record with incorrect number of columns
    def test_insert_record_incorrect_columns(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        result = query.insert(1, "John")
        assert result == False
        assert len(table.data) == 0

    # select a record with non-existing search key
    def test_select_non_existing_search_key(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        query.insert(1, "John", 25)
        result = query.select(2, 0, [1, 1, 1])
        assert result == []

    # delete a record with non-existing primary key
    def test_delete_non_existing_primary_key(self):
        table = Table("test_table", 3, 0)
        query = Query(table)
        query.insert(1, "John", 25)
        result = query.delete(2)
        assert result == False
        assert len(table.data) == 1