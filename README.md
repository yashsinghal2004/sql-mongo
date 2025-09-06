# SQL-Mongo Converter - A Lightweight SQL to MongoDB (and Vice Versa) Query Converter 

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat&logo=opensource)](LICENSE)  
[![Python Version](https://img.shields.io/badge/Python-%3E=3.7-brightgreen.svg?style=flat&logo=python)](https://www.python.org/)  
[![SQL](https://img.shields.io/badge/SQL-%23E34F26.svg?style=flat&logo=postgresql)](https://www.postgresql.org/)  
[![MongoDB](https://img.shields.io/badge/MongoDB-%23471240.svg?style=flat&logo=mongodb)](https://www.mongodb.com/)  
[![PyPI](https://img.shields.io/pypi/v/sql-mongo-converter.svg?style=flat&logo=pypi)](https://pypi.org/project/sql-mongo-converter/)

**SQL-Mongo Converter** is a lightweight Python library for converting SQL queries into MongoDB query dictionaries and converting MongoDB query dictionaries into SQL statements. It is designed for developers who need to quickly migrate or prototype between SQL-based and MongoDB-based data models without the overhead of a full ORM.

---

## Features

- **SQL to MongoDB Conversion:**  
  Convert SQL SELECT queries—including complex WHERE clauses with multiple conditions—into MongoDB query dictionaries with filters and projections.

- **MongoDB to SQL Conversion:**  
  Translate MongoDB find dictionaries, including support for comparison operators, logical operators, and list conditions, into SQL SELECT statements with WHERE clauses, ORDER BY, and optional LIMIT/OFFSET.

- **Extensible & Robust:**  
  Built to handle a wide range of query patterns. Easily extended to support additional SQL functions, advanced operators, and more complex query structures.

### Prerequisites

- Python 3.7 or higher
- pip
  
---  

## Usage

### Converting SQL to MongoDB

Use the `sql_to_mongo` function to convert a SQL SELECT query into a MongoDB query dictionary. The output dictionary contains:
- **collection:** The table name.
- **find:** The filter dictionary derived from the WHERE clause.
- **projection:** The columns to return (if not all).

#### Example

```python
from sql_mongo_converter import sql_to_mongo

sql_query = "SELECT name, age FROM users WHERE age > 30 AND name = 'Alice';"
mongo_query = sql_to_mongo(sql_query)
print(mongo_query)
# Expected output:
# {
#   "collection": "users",
#   "find": { "age": {"$gt": 30}, "name": "Alice" },
#   "projection": { "name": 1, "age": 1 }
# }
```

### Converting MongoDB to SQL

Use the `mongo_to_sql` function to convert a MongoDB query dictionary into a SQL SELECT statement. It supports operators such as `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, and `$regex`, as well as logical operators like `$and` and `$or`.

#### Example

```python
from sql_mongo_converter import mongo_to_sql

mongo_obj = {
    "collection": "users",
    "find": {
        "$or": [
            {"age": {"$gte": 25}},
            {"status": "ACTIVE"}
        ],
        "tags": {"$in": ["dev", "qa"]}
    },
    "projection": {"age": 1, "status": 1, "tags": 1},
    "sort": [("age", 1), ("name", -1)],
    "limit": 10,
    "skip": 5
}
sql_query = mongo_to_sql(mongo_obj)
print(sql_query)
# Example output:
# SELECT age, status, tags FROM users WHERE ((age >= 25) OR (status = 'ACTIVE')) AND (tags IN ('dev', 'qa'))
# ORDER BY age ASC, name DESC LIMIT 10 OFFSET 5;
```

---

## API Reference

### `sql_to_mongo(sql_query: str) -> dict`
- **Description:**  
  Parses a SQL SELECT query and converts it into a MongoDB query dictionary.
- **Parameters:**  
  - `sql_query`: A valid SQL SELECT query string.
- **Returns:**  
  A dictionary containing:
  - `collection`: The table name.
  - `find`: The filter derived from the WHERE clause.
  - `projection`: A dictionary specifying the columns to return.

### `mongo_to_sql(mongo_obj: dict) -> str`
- **Description:**  
  Converts a MongoDB query dictionary into a SQL SELECT statement.
- **Parameters:**  
  - `mongo_obj`: A dictionary representing a MongoDB find query, including keys such as `collection`, `find`, `projection`, `sort`, `limit`, and `skip`.
- **Returns:**  
  A SQL SELECT statement as a string.

---

## License

MIT License © 2025 [Yash Singhal]





