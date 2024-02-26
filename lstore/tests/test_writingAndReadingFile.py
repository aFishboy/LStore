from ..db import Database
from ..query import Query
from random import choice, randint, sample, seed
import pytest

class TestWriteAndReadFile:
    def test_writeFile(self):
        db = Database()
        print("here1")
        db.open("./ECS165A")
        db.create_table("Test1", 5, 0)
        db.close()
        print("here2")
        assert 1 == 2