# JDBC Sink Connector - Multi-Table Record Ordering Bug Fix

## Bug Summary

The JDBC Sink Connector fails to preserve Kafka record ordering when writing to multiple tables in a single batch, causing foreign key constraint violations.

## Root Cause

**File:** `src/main/java/io/confluent/connect/jdbc/sink/JdbcDbWriter.java`  
**Lines:** 69-85

The `write()` method uses a `HashMap<TableId, BufferedRecords>` to batch records per table. When flushing, the HashMap's iteration order is **non-deterministic**, breaking Kafka's partition-level ordering guarantee.

### Example Failure Scenario

```
Kafka records (in order):
  1. Parent record (id=1) → parents table
  2. Child record (parent_id=1) → children table
  3. Parent record (id=2) → parents table
  4. Child record (parent_id=2) → children table

Current (buggy) behavior:
  Step 1: Split into buffers
    parents: [record 1, record 3]
    children: [record 2, record 4]
  
  Step 2: HashMap.entrySet() iteration (random order)
    If children flushes first → FK constraint violation!
    Child records reference parents that don't exist yet
```

## Why Workarounds Were Needed

1. **Disabling FK constraints** → Loss of referential integrity
2. **Batch size = 1** → 10-100x performance degradation
3. Both are unacceptable for production systems

## The Fix

Changed from table-based batching to **ordered sequential processing**:

```java
// OLD (buggy): Collect all records per table, flush in random order
final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
for (SinkRecord record : records) {
  bufferByTable.get(tableId).add(record);
}
for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
  entry.getValue().flush(); // Random order!
}

// NEW (fixed): Process in order, flush when table changes
BufferedRecords currentBuffer = null;
TableId currentTableId = null;

for (SinkRecord record : records) {
  final TableId tableId = destinationTable(...);
  
  // Flush when switching tables (preserves order)
  if (currentBuffer != null && !tableId.equals(currentTableId)) {
    currentBuffer.flush();
    currentBuffer.close();
    currentBuffer = null;
  }
  
  if (currentBuffer == null) {
    currentBuffer = new BufferedRecords(...);
    currentTableId = tableId;
  }
  
  currentBuffer.add(record);
}

// Flush remaining records
if (currentBuffer != null) {
  currentBuffer.flush();
  currentBuffer.close();
}
```

## Benefits

1. **Preserves Kafka ordering** - Records written in exact partition order
2. **Respects FK constraints** - Parent records always written before children
3. **Maintains batching performance** - Consecutive records to same table still batched
4. **Deterministic behavior** - Same input always produces same result

## Performance Impact

**Best case:** No impact - all records go to same table (batches as before)  
**Worst case:** Alternating tables every record (same as batch.size=1, but correct)  
**Typical case:** Moderate improvement - most real-world data has runs of same table

## Test Coverage

Added test: `multiTableOrderingPreservedWithForeignKeys()`
- Creates parent/child tables with FK constraint
- Sends interleaved records (parent1, child1, parent2, child2)
- Verifies all records inserted without FK violations

## Migration Notes

This is a **bug fix**, not a breaking change:
- No configuration changes required
- Existing connectors get correct behavior automatically
- Performance may improve for sequential same-table records
- May expose previously masked data ordering issues

## Issue Details

**Severity:** Critical  
**Affects:** All multi-table sink configurations with FK constraints  
**Fixed in:** [Current commit]  
**Backwards compatible:** Yes
