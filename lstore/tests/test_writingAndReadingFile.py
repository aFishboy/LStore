from ..db import Database
from ..query import Query
from random import choice, randint, sample, seed
import pytest
from avltree import AvlTree


class TestWriteAndReadFile:
    def test_writeFile(self):
        # print(bytearray(4000))
        avl_tree = AvlTree()
        avl_tree[1] = [11,22,33]
        avl_tree[12] = [112,222,332]
        # print("avl\n", str(avl_tree), "\navl")
        testAvl = AvlTree({1: [11, 22, 33]}) 
        string_from_file = "{1: [11, 22, 33], 12: [112, 222, 332]}"
        dict_from_string = eval(string_from_file)
        newAVL = AvlTree(dict_from_string)
        # print("newAvl", str(newAVL), "newAvl")
        # print("avl2\n", str(testAvl), "\navl2")
        page_directory = {}
        page_directory[5] = (1, 2, 3)
        page_directory[15] = (11, 12, 13)
        page_directory[789] = (237, 22, 876)
        page_directory[55] = (11434, 3425, 422)

        # print("pd", page_directory)
        testAfter = {}
        testAfter = eval("{5: (1, 2, 3), 15: (11, 12, 13)}")
        # print("after\n", str(testAfter), "\nafter")
        # print(str(page_directory))
        db = Database()
        db.open("./ECS165A")
        table1 = db.get_table("Table1")
        print("Table1", table1)
        db.create_table("Table2", 5, 0)
        query = Query(table1)
        query.insert([0, 1])


        db.close()
        assert 1 == 2
