from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed, randrange
from time import process_time
import json
                            
try:
    # Student Id and 4 grades
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    keys = []

    insert_time_0 = process_time()
    for i in range(0, 10000):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = process_time()

    # Measuring update Performance
    update_cols = [
        [None, None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, 10000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 10000):
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])
    select_time_1 = process_time()

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 10000, 100):
        start_value = 906659671 + i
        end_value = start_value + 100
        result = query.sum(start_value, end_value - 1, randrange(0, 5))
    agg_time_1 = process_time()

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, 10000):
        query.delete(906659671 + i)
    delete_time_1 = process_time()

    # Prepare results
    results = {
        'insert_time': insert_time_1 - insert_time_0,
        'update_time': update_time_1 - update_time_0,
        'select_time': select_time_1 - select_time_0,
        'agg_time': agg_time_1 - agg_time_0,
        'delete_time': delete_time_1 - delete_time_0
    }


except Exception as e:
    results = {
        'insert_time': 0,
        'update_time': 0,
        'select_time': 0,
        'agg_time': 0,
        'delete_time': 0
    }
                            
records = {}
number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1
keys = {}

def reorganize_result(result):
    val = list()
    for r in result:
        val.append(r.columns)
    val.sort()
    return val

m2_count = 0
test_count = 0
m2_tests = {}
test_error = ""

records = [
    [0, 1, 1, 2, 1],
    [1, 1, 1, 1, 2],
    [2, 0, 3, 5, 1],
    [3, 1, 5, 1, 3],
    [4, 2, 7, 1, 1],
    [5, 1, 1, 1, 1],
    [6, 0, 9, 1, 0],
    [7, 1, 1, 1, 1],
]

db = Database()
db.open("./aa")
test_table = db.create_table('test', 5, 0)
query = Query(test_table)
for record in records:
    query.insert(*record)

# Test case 1
try:
    test_count += 1
    # select on columns with index
    test_table.index.create_index(2)
    result = reorganize_result(query.select(1, 2, [1,1,1,1,1]))
    if len(result) == 4:
        if records[0] in result and records[1] in result and records[5] in result and records[7] in result:
            pass
        else:
            test_error = "Select index test case failed. Record Not in Result"
    else:
        test_error = "Select index test case failed. Length Wrong"
except Exception as e:
    test_error = "Select index test case failed. Exception"
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select index finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 2
try:
    test_count += 1
    # select on columns without index and return 1 record
    test_table.index.drop_index(2)
    result = reorganize_result(query.select(3, 2, [1,1,1,1,1]))
    if len(result) == 1 and records[2] in result:
        pass
    else:
        test_error = "Select without index and return one record test case failed. result"
except Exception as e:
    test_error = "Select without index and return one record test case failed. Exception"
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select without index and return one record finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 3
try:
    test_count += 1
    # select on columns without index and return multiple records
    result = reorganize_result(query.select(1, 2, [1,1,1,1,1]))
    if len(result) == 4:
        if records[0] in result and records[1] in result and records[5] in result and records[7] in result:
            pass
        else:
            test_error = "Select without index and return multiple records test case failed."
    else:
        test_error = "Select without index and return multiple records test case failed."
except Exception as e:
    test_error = "Select without index and return multiple records test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select without index and return multiple records finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 4
try:
    test_count += 1
    # select on columns without index and return empty list
    result = reorganize_result(query.select(10, 2, [1,1,1,1,1]))
    if len(result) == 0:
        pass
    else:
        test_error = "Select without index and return empty list test case failed. len"
except Exception as e:
    test_error = "Select without index and return empty list test case failed. Exception"
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select without index and return empty list finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 5
try:
    test_count += 1
    # update on a primary key that does not exits
    query.update(8, *[None,2,2,2,2])
    result = reorganize_result(query.select(8, 0, [1,1,1,1,1]))
    if len(result) == 0:
        pass
    else:
        test_error = "Update on primary key that doesn't exist test case failed. Length"
except Exception as e:
    test_error = "Update on primary key that doesn't exist test case failed. Exception"
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Update on primary key that doesn't exist finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 6
try:
    test_count += 1
    # update that changes primary key,
    query.update(7, *[8,2,2,2,2])
    result = reorganize_result(query.select(7, 0, [1,1,1,1,1]))
    if len(result) == 0:
        pass
    else:
        test_error = "Update that changes primary key test case failed."
except Exception as e:
    test_error = "Update that changes primary key test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Update that changes primary key finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 7
try:
    test_count += 1
    # delete a record
    query.delete(5)
    result = reorganize_result(query.select(5, 0, [1,1,1,1,1]))
    if len(result) == 0:
        pass
    else:
        test_error = "Delete a record test case failed."
except Exception as e:
    test_error = "Delete a record test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Delete a record finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 8
try:
    test_count += 1
    # multiple tables

    test_table2 = db.create_table("test2", 5, 0)
    records2 = [
        [1, 1, 1, 2, 1],
        [2, 1, 1, 1, 2],
        [3, 0, 3, 5, 1],
        [4, 1, 5, 1, 3],
        [5, 2, 7, 1, 1],
        [6, 1, 1, 1, 1],
        [7, 0, 9, 1, 0],
        [8, 1, 1, 1, 1],
    ]
    query2 = Query(test_table2)
    for record in records2:
        query2.insert(*record)
    result = reorganize_result(query2.select(1, 0, [1,1,1,1,1]))
    if len(result) == 1 and records2[0] in result:
        pass
    else:
        test_error = "Multiple tables test case failed."
except Exception as e:
    test_error = "Multiple tables test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Multiple tables finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""


# Test case 9
try:
    test_count += 1
    # different primary key
    test_table3 = db.create_table("test3", 5, 2)
    records3 = [
        [1, 1, 0, 2, 1],
        [2, 1, 1, 1, 2],
        [3, 0, 2, 5, 1],
        [4, 1, 3, 1, 3],
        [5, 2, 4, 1, 1],
        [6, 1, 5, 1, 1],
        [7, 0, 6, 1, 0],
        [8, 1, 7, 1, 1],
    ]
    query3 = Query(test_table3)
    for record in records3:
        query3.insert(*record)
    result = query3.sum(3, 5, 4)
    if result == 5:
        pass
    else:
        test_error = "Different Primary Key test case failed."
except Exception as e:
    test_error = "Different Primary Key test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Different Primary Key finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""


def generte_keys():
    global records, number_of_records, number_of_aggregates, number_of_updates, keys
    
    if True:
        records = {}
        seed(3562901)

        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

        keys = sorted(records.keys())
        
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                for i in range(1, 5):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value

generte_keys()
if True:
    db = Database()
    db.open('./ab')
    # Create a table  with 5 columns
    #   Student Id and 4 grades
    #   The first argument is name of the table
    #   The second argument is the number of columns
    #   The third argument is determining the which columns will be primay key
    #       Here the first column would be student id and primary key
    grades_table = db.create_table('Grades', 5, 0)

    # create a query class for the grades table
    query = Query(grades_table)

    # dictionary for records to test the database: test directory
    records = {}

    seed(3562901)

    # Test case 10
    try:
        test_count += 1
        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            query.insert(*records[key])

    except Exception as e:
        test_error = "Insert test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Insert finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    # Test case 11
    try:
        test_count += 1
        # Check inserted records using select query
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                test_error = "Select test case failed."
            else:
                pass

    except Exception as e:
        test_error = "Select test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Select finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    # Test case 12
    try:
        test_count += 1
        # x update on every column
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                original = records[key].copy()
                for i in range(1, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value
                query.update(key, *updated_columns)
                record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                error = False
                for j, column in enumerate(record.columns):
                    if column != records[key][j]:
                        error = True
                if error:
                    test_error = "Update test case failed."
                else:
                    pass

    except Exception as e:
        test_error = "Update test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Update finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    # Test case 13
    try:
        test_count += 1
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum != result:
                test_error = "Aggregate test case failed."
            else:
                pass

    except Exception as e:
        test_error = "Aggregate test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Aggregate finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    db.close()

if True:
    db = Database()
    db.open('./ab')

    # Getting the existing Grades table
    grades_table = db.get_table('Grades')

    # create a query class for the grades table
    query = Query(grades_table)

    # dictionary for records to test the database: test directory

    # Test case 14
    try:
        test_count += 1
        # Check inserted records using select query
        err = False
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                err = True
                test_error = "Durability select test case failed."
            else:
                pass
        if not err:
            pass

    except Exception as e:
        test_error = "Durability select test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Durability select finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    # Test case 15
    try:
        test_count += 1
        err = False
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            correct_result = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            sum_result = query.sum(keys[r[0]], keys[r[1]], 0)
            if correct_result != sum_result:
                err = True
                test_error = "Durability aggregate test case failed."
            else:
                pass

    except Exception as e:
        test_error = "Durability aggregate test case failed."
    if test_error == "":
        m2_count += 1
        test_error = ""
        m2_tests[f"Test {test_count}"] = "Durability aggregate finished."
    else:
        m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
        test_error = ""

    db.close()       

output = {"tests": m2_tests, "count": m2_count, "total": test_count, "results": results}
           
print(json.dumps(output))