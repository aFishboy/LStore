from ..db import Database
from ..query import Query
from random import choice, randint, sample, seed
import pytest

class TestWriteAndReadFile:
    def test_writeFile(self):
        db = Database()
        db.close