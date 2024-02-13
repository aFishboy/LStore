import pytest
from ..table import Table
from ..table import Record
from ..query import Query

class TestSelectQuery:

    @pytest.fixture
    def setup_table(self):
        table = Table("test_table", 5, 0)  # Assuming 5 columns, first one being the primary key
        query = Query(table)
        return table, query

    def test_select_on_empty_table(self, setup_table):
        table, query = setup_table
        result = query.select(1, 0, [1, 1, 1, 1, 1])
        assert result == []

    def test_select_existing_record(self, setup_table):
        table, query = setup_table
        # Replace string values with integer representations
        query.insert(1, 2, 3, 4, 5)  # Assuming these integers are placeholder values
        result = query.select(1, 0, [1, 1, 1, 1, 1])
        expected = [Record(0, 1, [2, 3, 4, 5])]  # Adjusted to expect integers
        assert result == expected

    def test_select_with_non_existing_key(self, setup_table):
        table, query = setup_table
        query.insert(1, 2, 3, 4, 5)
        result = query.select(999, 0, [1, 1, 1, 1, 1])  # 999 does not exist
        assert result == []

    def test_select_specific_columns(self, setup_table):
        table, query = setup_table
        query.insert(1, 2, 3, 4, 5)
        # Assuming 0 represents excluding the column and 1 represents including it
        result = query.select(1, 0, [1, 0, 0, 0, 1])  # Selecting only the first and last columns
        expected = [Record(0, 1, [1, None, None, None, 5])]  # Adjust based on your Record class structure and handling of unspecified columns
        assert result == expected

    def test_select_multiple_records(self, setup_table):
        table, query = setup_table
        query.insert(1, 2, 3, 4, 5)
        query.insert(1, 2, 3, 4, 5)  # Duplicate primary key for testing
        result = query.select(1, 0, [1, 1, 1, 1, 1])
        assert len(result) == 2  # Expect 2 records with primary key = 1