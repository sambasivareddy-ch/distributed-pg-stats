# distributed-pg-stats
`distributed-pg-stats` is a **statistics-driven optimizer CLI for distributed PostgreSQL systems**.   

It collects **global cardinality statistics (NDV – Number of Distinct Values)** across tables and uses them to **compute optimized join orders** that reduce fan-out, result sizes, and expensive joins. This project is designed to work **outside PostgreSQL’s core planner**, acting as a **optimization layer**.

---

## Key Concepts
- **Global NDV (Number of Distinct Values)**  
  Approximate cardinality computed using HyperLogLog (HLL), mergeable across tables.

- **Join Order Optimization**  
  Joins are ordered from **smallest key-space → largest**, minimizing result size.

- **Planner**  
  PostgreSQL does not read external stats; instead, this tool **generates join paths and SQL** that guide execution.
---

## Features

- Collects global NDV stats per table/column
- Stores mergeable cardinality stats
- Computes join costs using NDV
- Generates optimized join order
- CLI-based (Cobra)
- Works with distributed PostgreSQL systems

---

## Entry Point (CLI Configuration)
This is the **first command you must run**.
### Usage
```bash
./distributed-pg-stats \
  -d database_name \
  -H host \
  -P port \
  -u username \
  -p password \
  -q meta_query
```
**What this does**:
- Connects to the PostgreSQL node
- Executes the provided meta_query to discover tables
- Generates a config.json file in the local directory
- This config is reused by all other commands

---
## Load Global NDV Statistics
This command computes global NDV (Number of Distinct Values) for each table and column discovered in the config.
### Usage
```bash
./distributed-pg-stats load-ndv
```
**What this does**
- Reads config.json
- Connects to the postgres
- Computes NDVs using HyperLogLog (HLL)
- Stores results in the global_ndv_stats table
> This step must be run before optimization.

## Optimize Join Order
This command uses global NDV stats to compute the best join order for a query.
### Usage
```bash
./distributed-pg-stats optimize \
  --table table1 \
  --table table2 \
  --table table3 \
  --join table1.col1=table2.col1 \
  --join table2.col2=table3.col2
```
### Output
```bash
Best Join order:
1. table2
2. table1
3. table3
```

## Current Limitations
- PostgreSQL planner is not modified
- Join order is generated externally
- No runtime feedback loop