## Implementing Sum and Update Operations

### Sum Operation

The `sum` operation aggregates values from a specific column within a range of keys. Here's the detailed implementation process:

#### Query Class

- **Start Point:** The `sum` function is initiated with parameters for the range of keys (`start_range`, `end_range`) and the column index to aggregate (`aggregate_column_index`).
- **Locate Records:** Iterates through the range, using the table's index to locate RIDs of records within the specified range.
- **Aggregate Values:** For each located RID, it calls the `Table` class to fetch the value from the specified column, summing these values.
- **Return Aggregate:** Returns the total sum to the caller.

#### Table Class

- **Retrieve Record Data:** On request, uses the RID to locate the page range and offset where the record is stored.
- **Read Column Value:** Reads the value of the specified column from the record, handling indirection if the record has been updated.

#### PageRange and Page Classes

- **Locate Record:** Accesses the correct page based on the offset and page range, reads the specified column's value from either the base or tail record, depending on indirection.

### Update Operation

Updating a record involves creating a tail record with updated values and updating pointers.

#### Query Class

- **Start Point:** Receives a primary key and new column values.
- **Delegate Update:** Passes the update operation to the `Table` class with the primary key and new values.

#### Table Class

- **Locate Record:** Locates the RID using the primary key.
- **Add Tail Record:** Creates a new tail record with updated values. Unchanged values are copied from the current record.
- **Update Indirection:** Updates the base record's indirection pointer to point to the new tail record.
- **Update Page Directory:** Reflects the new tail record's location if necessary.

#### PageRange and Page Classes

- **Tail Record Creation:** Adds the new tail record to an appropriate page, updates metadata such as indirection pointers, and handles schema encoding for updated columns.

#### Index and Locking

- **Index Updates:** Updates indexes if indexed column values change.
- **Concurrency Control:** Implements locking to ensure safe execution of updates in a multi-threaded environment.

### Detailed Workflow

1. **Query Execution:** The `Query` class serves as the entry point for executing `sum` or `update` operations.
2. **Data Retrieval and Modification:** The `Table` class orchestrates operations involving record location, data reading, and updates.
3. **Data Storage:** Interacts with `PageRange` and `PageBlock` or `Page` classes for physical data storage access and modification.
4. **Metadata Management:** Updates metadata such as indexes, indirection pointers, and schema encodings to maintain data integrity and reflect the current state.