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


db = Database()
db.open('./ECS165')
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

m2_count = 0
test_count = 0
m2_tests = {}
test_error = ""

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1

seed(3562901)

# Test case 1
try:
    test_count += 1
    for i in range(0, number_of_records):
        key = 92106429 + i
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    keys = sorted(list(records.keys()))
except Exception as e:
    test_error = "Insert test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Insert finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 2
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

# Test case 3
try:
    test_count += 1
    for _ in range(number_of_updates):
        for key in keys:
            updated_columns = [None, None, None, None, None]
            # copy record to check
            original = records[key].copy()
            for i in range(2, grades_table.num_columns):
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

# Test case 4
try:
    test_count += 1
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], 0)
        if column_sum != result:
            test_error = "Aggregrate test case failed."
        else:
            pass

except Exception as e:
    test_error = "Aggregrate test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Aggregrate finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

db.close()


db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1

seed(3562901)
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

# Simulate updates
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value
keys = sorted(list(records.keys()))

# Test case 5
try:
    test_count += 1
    # Check records that were presisted in part 1
    for key in keys:
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for i, column in enumerate(record.columns):
            # print("column", column, "records[key][i]", records[key][i], column != records[key][i])
            if column != records[key][i]:
                error = True
        if error:
            test_error = "Select for version -1 test case failed. Error"

except Exception as e:
    test_error = "Select for version -1 test case failed. Exception"
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select for version -1 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 6
try:
    test_count += 1
    for key in keys:
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
        error = False
        for i, column in enumerate(record.columns):
            # print("column", column, "records[key][i]", records[key][i], column != records[key][i])
            if column != records[key][i]:
                error = True
        if error:
            test_error = "Select for version -2 test case failed."

except Exception as e:
    test_error = "Select for version -2 test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select for version -2 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 7
try:
    test_count += 1
    for key in keys:
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != updated_records[key][i]:
                error = True
        if error:
            test_error = "Select for version 0 test case failed."

except Exception as e:
    test_error = "Select for version 0 test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Select for version 0 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 8
try:
    test_count += 1
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
        if column_sum != result:
            test_error = "Aggregate for version -1 test case failed."

except Exception as e:
    test_error = "Aggregate for version -1 test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Aggregate for version -1 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 9
try:
    test_count += 1
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
        if column_sum != result:
            test_error = "Aggregate for version -2 test case failed."

except Exception as e:
    test_error = "Aggregate for version -2 test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Aggregate for version -2 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 10
try:
    test_count += 1
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        updated_column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
        updated_result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
        if updated_column_sum != updated_result:
            test_error = "Aggregate for version 0 test case failed."

except Exception as e:
    test_error = "Aggregate for version 0 test case failed."
if test_error == "":
    m2_count += 1
    test_error = ""
    m2_tests[f"Test {test_count}"] = "Aggregate for version 0 finished."
else:
    m2_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)

db.close()
                                    
output = {"tests": m2_tests, "count": m2_count, "total": test_count, "results": results}
           
print(json.dumps(output))