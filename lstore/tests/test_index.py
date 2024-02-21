from ..db import Database
from ..index import Index
from avltree import AvlTree
from ..page import Page
from random import choice, randint, sample, seed
import pytest

@pytest.fixture
def index_fixture():
    # Mock or setup your Database/Table structure as needed
    # For example, if Index expects a table object with num_columns and a key attribute
    class MockTable:
        def __init__(self, num_columns, key):
            self.num_columns = num_columns
            self.key = key
    # Mock setup for demonstration, adjust according to actual Index class requirements
    mock_table = MockTable(num_columns=3, key=0)
    index = Index(mock_table)
    return index

class TestIndex:
    def test_init(self, index_fixture):
        """
        Test if the Index class is initialized correctly.
        """
        assert index_fixture.key == 0
        assert isinstance(index_fixture.indices[0], AvlTree)

    def test_locate(self, index_fixture):
        """
        Test locating a single value in a column.
        """
        # Simulating inserting data into the index
        mock_value_location = [123] 
        index_fixture.indices[1].insert(1, mock_value_location)
        assert index_fixture.locate(1, 1) == mock_value_location

    def test_locate_range(self, index_fixture):
        """
        Test locating a range of values in a column.
        """
        # Simulating inserting data into the index
        mock_value_locations = [123, 456]
        for value, location in zip([2, 3], [[loc] for loc in mock_value_locations]):
            index_fixture.indices[1].insert(value, [location]) 
        assert index_fixture.locate_range(2, 3, 1) == mock_value_locations