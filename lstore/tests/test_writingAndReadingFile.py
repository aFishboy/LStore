from ..db import Database
from ..query import Query
from random import choice, randint, sample, seed
import pytest

class TestWriteAndReadFile:
    def test_writeFile(self):
        db = Database()
        db.open("./ECS165A")
        db.create_table("Table1", 5, 0)
        db.close()
        assert 1 == 2