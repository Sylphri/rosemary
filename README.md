# Database
### Software in process of development. Don't have any high expectations.
# Quick Start

```console
$ cargo run
```

## Database Reference

For now database support work with one table. Structure of the table can be changed with table schema file.
> Schema file have next structure
```file
<table_name>
<column_name>:<column_type>
<column_name>:<column_type>
<column_name>:<column_type>
...
```

## Column types

Database support integers and strings with max length of 50.

Example:
```file
table
id:Int
name:Str
```

## Literals

### Integer

Currently an integer is a sequence of decimal digits that optionally starts with a dash (-) to indicate a negative integer. 

Example:
```console
query > * select id 3 == filter-and
```

### String

Any sequence of characters that contain not only decimal digits will be a string.

Example:
```console
query > name select
```

## Operations

### select

The `select` operation is used to select data from a table.

`select` syntax
```console
<column_name> <column_name> ... select
```

Example:
```console
query > id name select
    0                John
    1              Alexey
```

### insert

The `insert` operation is used to insert new records in a table. Provided arguments must be the same type as columns and in corresponding order.

`insert` syntax
```console
<arg> <arg> ... insert
```

Example:
```console
query > 0 Emily insert
```

### delete

The `delete` operation is used to delete existing records in a table. `delete` operation expect a column name and a value, all rows with that value will be deleted.

`delete` syntax
```console
<column_name> <value> delete
```

Example:
```console
query > id 5 delete
```

### filter-and

The `filter-and` operation is used to extract only those records that fulfill a specified condition. `filter-and` operation expect a sequence of conditions and must be used after `select` operation. The operation displays a record if all conditions is true.

`filter-and` syntax
```console
* select <condition> <condition> filter-and
```

Example:
```console
query > * select id 3 > name == John filter-and
```

### filter-or

The `filter-or` acts like a `filter-and` operation but displays a record if any of the conditions is true.

`filter-or` syntax
```console
* select <condition> <condition> filter-or
```

Example:
```console
query > * select id 3 > name == John filter-or
```

## Conditions

Conditions used with `filter-and` and `filter-or` operations. All of them have the same syntax.

Currently supported four conditions: `==` (equal), `!=` (not equal), `<` (less than), `>` (more than).

Conditions syntax
```console
<column_name> <value> condition
```

## Usage

To provide a query go to query mode with command `query`. `query > ` prompt indicates that you in query mode.
```console
> query
query >
```

To exit from query mode or program use `exit` command.
```console
query > exit
>
```

To be able to write mulitline queries put a `(` in query to go to muliline query mode. `query : ` prompt indicates that you in multiline query mode.
In this mode all lines will be executed as one query.
```console
query > ( * select
query :
```

To end multiline query put a `)` in a query.
```console
query > ( * select
query :   id 5 == filter-and )
query >
```

### filters combination

You can combine several `filter-and` and `filter-or` operation ot produce complicated conditions.

Example:
```console
query > ( * select
query :   id 2 > id 5 < filter-and
query :   name John == name Emily == filter-or )
```
