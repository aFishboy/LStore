from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

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

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

m3_count = 0
test_count = 0
m3_tests = {}
test_error = ""

number_of_records = 1000
number_of_transactions = 100
num_threads = 8

# Test case 1
try:
    test_count += 1
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    test_error = "Index API test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Index API test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

# Test case 2
try:
    test_count += 1
    for i in range(number_of_transactions):
        insert_transactions.append(Transaction())

    for i in range(0, number_of_records):
        key = 92106429 + i
        keys.append(key)
        records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
        t = insert_transactions[i % number_of_transactions]
        t.add_query(query.insert, grades_table, *records[key])

    transaction_workers = []
    for i in range(num_threads):
        transaction_workers.append(TransactionWorker())
        
    for i in range(number_of_transactions):
        transaction_workers[i % num_threads].add_transaction(insert_transactions[i])

    # run transaction workers
    for i in range(num_threads):
        transaction_workers[i].run()

    # wait for workers to finish
    for i in range(num_threads):
        transaction_workers[i].join()

    # Check inserted records using select query in the main thread outside workers
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
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Select test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
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
number_of_transactions = 100
number_of_operations_per_record = 1
num_threads = 8

keys = []
records = {}
seed(3562901)

# Test case 3
try:
    test_count += 1
    # re-generate records for testing
    for i in range(0, number_of_records):
        key = 92106429 + i
        keys.append(key)
        records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]

    transaction_workers = []
    transactions = []

    for i in range(number_of_transactions):
        transactions.append(Transaction())

    for i in range(num_threads):
        transaction_workers.append(TransactionWorker())

    updated_records = {}
    # x update on every column
    for j in range(number_of_operations_per_record):
        for key in keys:
            updated_columns = [None, None, None, None, None]
            updated_records[key] = records[key].copy()
            for i in range(2, grades_table.num_columns):
                # updated value
                value = randint(0, 20)
                updated_columns[i] = value
                # update our test directory
                updated_records[key][i] = value
            transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
            transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)

except Exception as e:
    test_error = "Update test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Update test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""


# add trasactions to transaction workers  
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(transactions[i])

# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

# Test case 4
try:
    test_count += 1
    score = len(keys)
    for key in keys:
        correct = records[key]
        query = Query(grades_table)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
        if correct != result:
            test_error = "Select version -1 test case failed."
            score -= 1

except Exception as e:
    test_error = "Select version -1 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Select version -1 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 5
try:
    test_count += 1
    v2_score = len(keys)
    for key in keys:
        correct = records[key]
        query = Query(grades_table)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0].columns
        if correct != result:
            test_error = "Select version -2 test case failed."
            v2_score -= 1

except Exception as e:
    test_error = "Select version -2 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Select version -2 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 6
try:
    test_count += 1
    if score != v2_score:
        test_error = "Select score matching test case failed."

except Exception as e:
    test_error = "Select score matching test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Select score matching test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 7
try:
    test_count += 1
    score = len(keys)
    for key in keys:
        correct = updated_records[key]
        query = Query(grades_table)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0].columns
        if correct != result:
            test_error = "Select version 0 test case failed."
            score -= 1

except Exception as e:
    test_error = "Select version 0 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Select version 0 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 8
try:
    test_count += 1
    number_of_aggregates = 100
    valid_sums = 0
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
        if column_sum == result:
            valid_sums += 1

except Exception as e:
    test_error = "Aggregate version -1 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Aggregate version -1 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 9
try:
    test_count += 1
    v2_valid_sums = 0
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
        if column_sum == result:
            v2_valid_sums += 1
    
except Exception as e:
    test_error = "Aggregate version -2 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Aggregate version -2 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 10
try:
    test_count += 1
    if valid_sums != v2_valid_sums:
        test_error = "Aggregrate score matching test case failed."

except Exception as e:
    test_error = "Aggregrate score matching test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Aggregrate score matching test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

# Test case 11
try:
    test_count += 1
    valid_sums = 0
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
        if column_sum == result:
            valid_sums += 1

except Exception as e:
    test_error = "Aggregate version 0 test case failed."
if test_error == "":
    m3_count += 1
    test_error = ""
    m3_tests[f"Test {test_count}"] = "Aggregate version 0 test case finished."
else:
    m3_tests[f"Test {test_count}"] = f"Error: {test_error}"
    test_error = ""

db.close()

output = {"tests": m3_tests, "count": m3_count, "total": test_count, "results": results}
           
print(json.dumps(output))