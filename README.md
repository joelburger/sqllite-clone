# SQLite Database Reader

This project is a JavaScript-based SQLite database reader. It allows you to read and query SQLite database files using Node.js. The project includes functions to read database headers, schemas, tables and indexes, as well as to execute SQL commands like `SELECT`.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Basic Concepts](#basic-concepts)
- [References](#references)

## Usage

To run the project, use the following command:

```bash
node main.js <database-file> <command>
```

### Commands

- `.dbinfo`: Displays database information such as page size and number of tables.
- `.tables`: Lists all user tables in the database.
- `SELECT`: Executes a `SELECT` query on the database.

## Basic Concepts

### Page Types

- **Leaf Page (`0x0d`)**: Contains actual table rows
- **Interior Page (`0x05`)**: Contains pointers to other pages containing table rows
- **Leaf Index (`0x0a`)**: Contains actual table indexes
- **Interior Page (`0x02`)**: Contains pointers to other pages containing table indexes

### Functions

- **`fetchPage`**: Reads a specific page from the database file.
- **`parsePageHeader`**: Parses the header of a database page.
- **`readIndexPage`**: Reads an index page and filters keys based on a given value.
- **`indexScan`**: Scans an index to retrieve rows matching certain criteria.
- **`tableScan`**: Scans a table to retrieve all rows.

### SQL Parsing

- **`parseSelectCommand`**: Parses a `SELECT` SQL command to extract columns, table name, and where clause.
- **`parseColumns`**: Parses the columns from a table schema.
- **`searchIndex`**: Searches for an index that matches the query criteria.

## References

- [SQLite Database File Format](https://www.sqlite.org/fileformat.html)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
