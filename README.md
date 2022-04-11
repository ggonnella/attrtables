AttrTables is a library for creating a system for storing,
a dynamic set of attributes for entities in a database.

Each _entity_ (identified by an unique ID) can have as
many _attributes_ as desired. These consist in one or multiple
values, each stored in an independent database column.
New attributes can be added at any time.

Alongside with the values, attributes can store a computation
ID (which can refer to an external table of computation metadata).
Computation IDs can be stored for individual attributes and
for groups of attributes.

The attribute columns are automatically spread among multiple
database tables, so that the total number of columns does
not exceed a given limit.
