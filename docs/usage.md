# AttrTables: Usage manual

The main class of the library is ``AttributeValueTables``, which represents the
collection of tables, where the attributes are stored.

## Creating the AttributeValueTables instance

An instance of the class is needed for all operations. The constructor
requires, as a mandatory parameter, a SqlAlchemy _connectable_. The engine must
be used with ``future=True``.

For example:
```
engine = create_engine(connection_string, future=True)
connection = engine.connect()
avt = AttributeValueTables(connection)
```

Besides the connectable, further arguments can be passed to the constructor,
explained below.

### Computation IDs

By default, for each attribute, computation IDs can be stored alongside the
attribute values, so that the computation metadata can be stored (e.g. in a
different table).

The type of the computation ID column is by default ``binary(16)`` (which can
be used for storing a UUID). It can be set to a different type by setting the
``computation_id_type`` argument. If the computation IDs are not needed, the
``support_computation_ids`` argument can be set to False.

When computation IDs are supported, computation groups can be defined, which
group attributes together. If for an entity, all attributes of a group are
computed at once, the computation ID of the group is stored instead of the
single IDs, saving place (since all the computation IDs of the attributes will
be set to NULL). To disable attribute computation groups set
``support_computation_groups`` to ``False``.

### Value tables

Attributes values are stored in multiple tables. The attributes are assigned
automatically to tables, based on the number of columns required to store the
values and computation ids.

The target number of columns per table is a parameter that can be set by
setting the ``target_n_columns`` argument. The default is 64. Generally, the
number of columns per table does not exceed this value. Only if a given
attribute alone already requires a number of columns larger than this, the
table for that attribute will have more columns that this value.

The names of the attribute value tables all have a common prefix, which can be
passed as the ``tablename_prefix`` argument.

### Attribute definitions table

A table for storing attribute names and database (and, optionally, attribute
computation groups) is needed. A database model for such a class is provided
(``AttributeDefinition``) and used by default by ``AttributeValueTables``.

Optionally, the table can be defined using a different model (for example for
defining further columns). In this case the model class is passed as the
``attrdef_class`` attribute to the ``AttributeValueTables`` constructor. The
class must be a SqlAlchemy ORM model providing at least the attributes ``name``
and ``datatypes``. If the computation groups are enabled (by default) also
``computation_group`` must be provided.

## Creating attributes

Before values can be stored for an attribute, the attribute must be created.

This is done using the ``create_attribute()`` method of the
``AttributeValueTables`` instance.

The method has two mandatory arguments:
- ``name``: must consist of letters, digits and underscores and may not start
  with a digit.
- ``datatype``: a string describing the datatype

If computation groups are enabled (default), each attribute can be assigned to
a computation group (not mandatory), by setting the ``computation_group``
argument.

If a different model for attribute definitions is used, which contain further
columns, the values for these columns can be passed to ``create_attribute()``
using keyword arguments.

### Datatype description

The datatype is described using SqlAlchemy column types
(see e.g. https://docs.sqlalchemy.org/en/14/core/type_basics.html).

For example:
- ``"Boolean"``
- ``"Integer"``
- ``"Float"``
- ``"String(n)"`` where n is an integer >= 1, e.g.
  ``"String(50)"``
- ``"Text"``

An attribute can consist in a single value, in which case
the datatype is just a string containing such a column type name.

Furthermore, attributes can contain multiple values.
If the values have different datatypes, they are joined using ";".
For example:
- ``"Boolean;Integer;String(50)"``

If an arary of values of the same type is desired, this can be
specified, by adding a ``[n]`` suffix, e.g.
- ``Integer[10]``
- ``String(50)[10]``

These can be used also in combined definitions with ";", e.g.
- ``Boolean;Integer[3];Float;String(50)[4]``

### Attribute name reccomendations

Since in some systems the column names are case insensitive, it is
recommended to use lower case letters.

The length of the name is limited by two factors:
- first, the length of the name column in the attribute definition table (by
  default 62).
- second, the maximum length of a column name in the database;
  it shall be remarked that a suffix is appended to the attribute name
  (e.g. ``_v``):
  - for scalar attributes the suffixes have length 2;
  - for composed/array attributes, the suffix has
    length ``2+ceil(log10(n_elements))`` (e.g. 4 for 100 elements)

## Setting values of an attribute

Once an attribute has been added, values of the attribute for a number of
entities can be set using the ``set_attribute()`` method of the
AttributeValueTables instance:
```
avt.set_attribute(attribute_name, values_dictionary, computation_id)
```

Thereby:
- ``attribute_name`` is the name of the attribute (which must have been already
added, using ``create_attribute()``, see above)
- ``values_dictionary`` is a dictionary of ``{entity_id: values}``,
where values is either a scalar (for scalar attributes) or a list of values
(for compound/array attributes; it must have the correct size in that case)
- ``computation_id`` is not mandatory (and it may only be provided if support
  for computation IDs is not disabled)

### Setting multiple attributes at once

If multiple attributes are computed at once, they can be set using the
``set_attributes()`` method of the AttributeValueTables instance:
```
avt.set_attributes(attribute_names, values_dictonary, computation_id)
```

Thereby the ``attribute_names`` is a list of names of attributes. The values
dictionary and computation ID have the same meaning as when adding a single
attribute. However, the lists of values in the entries of the
``values_dicionary`` must in this case contain one element for each of the
columns of the attributes in ``attribute_names``, in the correct order. E.g.
```
avt.create_attribute("a", "Integer[2]")
avt.create_attribute("b", "Float[2]")
avt.set_attributes(["a", "b"], {"entity1": [1, 2, 1.1, 2.2]})
```

### Loading the results of a batch computation

For performance reasons, the results of a batch computation can be directly
loaded from a tab-separated file. This is done using the ``load_computation``
method of the AttributeValueTables instance:
```
avt.load_computation(computation_id, attributes, inputfile)
```

Thereby a temporary table is created, the data is loaded to the table
and then merged with the original tables (the temporary table name suffix
is ``temporary`` and can be set to a different value
using the keyword argument ``tmpsfx``).

The inputfile must contain a number of columns and datatypes compatible with
the list of attributes, e.g.
```
avt.create_attribute("a", "Integer[2]")
avt.create_attribute("b", "Float[2]")
avt.load_computation(computation_id1, ["a", "b"], "results_file.tsv")

# where results_file.tsv contains, e.g.:
entity1   1   2   1.1    2.2
entity2   2   3   2.3    3.1
```

## Deleting an attribute value

To remove the value of an attribute for some entities, the ``unset_attribute``
method of the AttributeValueTables instance is used:
```
avt.unset_attribute(attribute_name, [list_of_entity_ids])
```

## Querying the values of an attribute

To query the values of an attributes for a list of entities, the
``query_attribute`` method of the AttributeValueTables instance is used:
```
avt.query_attribute(attribute_name, [list_of_entity_ids])
```

The return value is a dictionary, with an entry for each entity of the list for
which a value of the attribute exists. The value of the dictionary entry is a
tuple ``(attribute_value, computation_id)`` if computation IDs are supported
(default) or just ``attribute_value`` otherwise. Thereby ``attribute_value`` is
a scalar, if the attribute is scalar, and is a tuple for compound/arrray
attributes.

## Destroying an attribute

To destroy an attribute the following method of the AttributeValueTables
instance is used:
```
avt.destroy_attribute(attribute_name)
```
