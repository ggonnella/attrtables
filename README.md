# AttrTables

AttrTables is a library for storing attributes of entities in a database,
where the entities are rows and the attributes are columns in an automatically
managed set of tables.

## Basic concepts

Each _entity_ (identified by an unique ID) can have as many _attributes_ as
desired. These consist in one or multiple values, each stored in an independent
database column. New attributes can be added at any time.

Alongside with the values, attributes can store a computation ID (which can
refer to an external table of computation metadata). Computation IDs can be
stored for individual attributes and optionally also for groups of attributes.

The attribute columns are automatically spread among multiple database tables,
so that the total number of columns does not exceed a given limit.

## Comparison to entity-attribute-value

An alternative to the model implemented by AttrTables is the
entity-attribute-value (EAV) model, in which there is a single table, where
entities are rows and the attribute name and value are two columns.

The EAV model has some disadvantages, compared to AttrTables:
- the values of a single attribute generally cannot be indexed
- the values must be stored in a generic data type (such as blobs),
  and the application must convert back and forth to the correct
  datatype

## Setup
The library is based on SqlAlchemy, which must be installed (see
``requirements.txt``).

Furthermore, a database must be setup. The connection to the database is done
using SqlAlchemy, and the connectable is passed to the library, as explained
below.

## Running the tests suite

To run the test suite, a database is needed, where the test tables can be
stored. The user must create such database.

The database configuration is provided using a YAML configuration file
"config.yaml", which shall be stored in the "tests" subdirectory (see as an
example "tests/config.yaml").

## Usage

The usage of the library is explained in the
 [user manual](https://github.com/ggonnella/attrtables/blob/main/docs/usage.md).
