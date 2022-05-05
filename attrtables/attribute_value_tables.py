#
# (c) 2022 Giorgio Gonnella, University of Goettingen, Germany
#
"""
DB Schema for tables storing attribute values.
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Table, inspect, select, text, MetaData
import sqlalchemy.types
from sqlalchemy.orm import Session
from sqlalchemy_repr import PrettyRepresentableBase
import ast
from collections import Counter
from attrtables.attribute_value_mixin import AttributeValueMixin
from attrtables.attribute_definition import AttributeDefinition

Base = declarative_base(cls=PrettyRepresentableBase)

class AttributeValueTables():

  def tablename(self, sfx):
    return self.tablename_prefix + str(sfx)

  @staticmethod
  def normalize_suffix(sfx):
    return str(sfx).upper()

  DEFAULT_TARGET_N_COLUMNS = 64
  DEFAULT_COMPUTATION_ID_TYPE = sqlalchemy.types.BINARY(16)
  DEFAULT_ENTITY_ID_TYPE = sqlalchemy.types.String(64)
  DEFAULT_TABLENAME_PREFIX = "attribute_value_t"
  VALUE_COLUMN_SUFFIX = "_v"
  COMPUTATION_COLUMN_SUFFIX = "_c"
  COMPUTATION_GROUP_COLUMN_SUFFIX = "_g"

  def _init_attributes_maps(self, inspector, session):
    self._a2t = {}   # {attr_name: tab_sfx}
    self._t2a = {}   # {tab_sfx: Counter{attr_name: nof_v_cols}}
    self._t2g = {}   # {tab_sfx: {group_name: [attr_names]}}
    self._ncols = {} # {tax_sfx: total_nof_columns (entity_id + all v/c/g)}
    for tn in inspector.get_table_names():
      if tn.startswith(self.tablename_prefix):
        cnames = [c["name"] for c in inspector.get_columns(tn)]
        sfx = tn[len(self.tablename_prefix):]
        self._ncols[sfx] = len(cnames)
        cnames.remove("entity_id")
        anames = Counter(cn.rsplit("_", 1)[0] for cn in cnames \
            if not cn.endswith(self.COMPUTATION_COLUMN_SUFFIX) and \
               not cn.endswith(self.COMPUTATION_GROUP_COLUMN_SUFFIX))
        self._t2a[sfx] = anames
        for an in anames:
          if an in self._a2t:
            raise RuntimeError(f"Attribute {an} found in multiple tables\n"+\
                               f"({self.tablename(self._a2t[an])} and {tn})")
          self._a2t[an] = sfx
        if self.support_computation_groups:
          gnames = set(cn.rsplit("_", 1)[0] for cn in cnames \
              if cn.endswith(self.COMPUTATION_GROUP_COLUMN_SUFFIX))
          self._t2g[sfx] = {gname: set(session.execute(\
                select(self.attrdef_class.name).filter(\
              self.attrdef_class.computation_group == gname,
              self.attrdef_class.name.in_(anames))).scalars().all()) \
                  for gname in gnames}

  def __init__(self, connectable,
               attrdef_class = AttributeDefinition,
               entity_id_type = DEFAULT_ENTITY_ID_TYPE,
               tablename_prefix = DEFAULT_TABLENAME_PREFIX,
               target_n_columns = DEFAULT_TARGET_N_COLUMNS,
               support_computation_ids = True,
               support_computation_groups = True,
               computation_id_type = DEFAULT_COMPUTATION_ID_TYPE):
    """
    Initialize attribute value tables

    Args:
      connectable:  SQLAlchemy connectable
      attrdef_class: class from which to access the attribute definitions
      tablenamepfx: common table name prefix for each of the tables

    ## Connectable

    The connectable attribute can be set to an engine or a connection
    (with future=True).

    Using a connection is useful, to wrap everything an instance does into
    a transaction, by setting it to a connection in a transaction, e.g.:

      engine = create_engine(...)
      with engine.connect() as connection:
        with connection.begin():
          avt = AttributeValueTables(connection)
          # ...
          session = Session(bind=connection)
          # ... use the same transaction also for other ORM operations

    ## Attribute definitions

    The attribute definitions are used to store the column names and
    datatypes for the attribute value tables (plus any other metadata
    that is needed for applications external to this module).

    The attribute definitions are stored using a separate class, which can be
    passed as the attrdef_class argument. The default is to use the
    AttributeDefinition class.

    ## Tables name prefix

    The names of the attribute value tables all have a common prefix,
    which can be passed as the "tablenamepfx" argument.

    ## Target number of columns per table

    Attributes values are stored in multiple tables. The attributes are
    assigned automatically to tables, based on the number of columns
    required to store the values and computation ids.

    The target number of columns per table is a parameter that can be set by
    setting the "target_n_columns" argument. The default is 64. Generally, the
    number of columns per table does not exceed this value. Only if a given
    attribute alone already requires a number of columns larger than this, the
    table for that attribute will have more columns that this value.

    ## Computation IDs

    The computation IDs are stored in the attribute value tables as a
    separate column. The computation IDs are used to identify the
    computation that produced the attribute value.

    If the computation IDs are not needed, the support_computation_ids
    argument can be set to False.

    The type of the computation ID column is by default binary(16), but
    can be set to a different type by setting the computation_id_type
    argument.

    ## Attribute computation groups

    The computation groups are used to group attributes together, so that
    if they are computed together, less space is used by the computation IDs.
    The computation groups for each attribute are defined in the attribute
    definitions table.

    If support_computation_ids is set to False or support_computation_groups
    is set to False, then computation groups are not used.
    """
    self.connectable = connectable
    self.attrdef_class = attrdef_class
    self.attrdef_class.metadata.bind = connectable
    self.attrdef_class.metadata.create_all()
    self.tablename_prefix = tablename_prefix
    self.computation_id_type = computation_id_type
    self.entity_id_type = entity_id_type
    self.target_n_columns = target_n_columns
    self.support_computation_ids = support_computation_ids
    self.support_computation_groups = support_computation_ids and \
                                        support_computation_groups
    self._drop_temporary()
    with Session(connectable) as session:
      inspector = inspect(connectable)
      self._init_attributes_maps(inspector, session)

  @property
  def table_suffixes(self):
    return list(self._t2a.keys())

  @property
  def attribute_names(self):
    return list(self._a2t.keys())

  def table_for_attribute(self, attribute):
    return self.tablename(self._a2t[attribute])

  def attribute_group(self, attribute, t_sfx):
    if self.support_computation_groups:
      for g_name, g_members in self._t2g[t_sfx].items():
        if attribute in g_members:
          return g_name
    return None

  def attribute_location(self, attribute):
    """
    Table suffix and column names of an attribute
    """
    t_sfx = self._a2t[attribute]
    vcolnames = self._vcolnames(attribute, self._t2a[t_sfx][attribute])
    ccolname = None
    if self.support_computation_ids:
      ccolname = self._ccolname(attribute)
    gcolname = None
    if self.support_computation_groups:
      gcolname = self._gcolname(attribute, t_sfx)
      grp = self.attribute_group(attribute, t_sfx)
      gcolname = self._gcolname(grp) if grp else None
    return (t_sfx, vcolnames, ccolname, gcolname)

  def attribute_class(self, attribute):
    t_sfx = self._a2t.get(attribute, None)
    return self.get_class(t_sfx) if t_sfx else None

  def attribute_value_columns(self, attribute):
    t_sfx = self._a2t.get(attribute, None)
    return self._vcolnames(attribute, self._t2a[t_sfx][attribute]) \
        if t_sfx else None

  def attribute_computation_column(self, attribute):
    if self.support_computation_ids:
      t_sfx = self._a2t.get(attribute, None)
      return self._ccolname(attribute) if t_sfx else None
    else:
      return None

  def attribute_computation_group_column(self, attribute):
    grp = None
    if self.support_computation_groups:
      t_sfx = self._a2t.get(attribute, None)
      if t_sfx:
        grp = self.attribute_group(attribute, t_sfx)
    return self._gcolname(grp) if grp else None

  def attribute_access_data(self, attribute):
    """
    Access data for an attribute.

    Return value:
      (class for table of attribute, value column names, computation ID
       column name, computation group column name)

    If the attribute is not defined, the class will be None.

    If computation IDs are not supported, the computation ID column name
    will be None. If computation groups are not supported or the attribute
    does not belong to a computation group, the computation group column name
    will be None.

    """
    klass = self.attribute_class(attribute)
    if klass is None:
      return (None, None, None, None)
    vcols = self.attribute_value_columns(attribute)
    ccolname = self.attribute_computation_column(attribute)
    gcolname = self.attribute_computation_group_column(attribute)
    return (klass, vcols, ccolname, gcolname)

  def query_attribute(self, attribute, entity_ids = None):
    """
    Query attribute values for a list of entity ids

    Arguments:
      attribute: name of the attribute
      entity_ids: list of entity ids
                  (if none: all entities are queried)

    If computation IDs are enabled (default):
      a dictionary ``{entity_id: (attribute_values, computation_id)``},
      where:
      - ``entity_id``: is one of the entity ids in the input
      - ``attribute_values``: depending on the attribute datatype,
        it is either a single value or a tuple of values
      - ``computation_id``: the id of the computation that
        produced the attribute value (or None if it was not recorded)

    If computation IDs are disabled:
      a dictionary ``{entity_id: attribute_values}``, where:
      - ``entity_id``: is one of the entity ids in the input
      - ``attribute_values``: depending on the attribute datatype,
        it is either a single value or a tuple of values
    """
    klass, vcolnames, ccolname, gcolname = self.attribute_access_data(attribute)
    session = Session(self.connectable)
    if entity_ids:
      rows = session.query(klass).filter(klass.entity_id.in_(entity_ids)).all()
    else:
      rows = session.query(klass).all()
    results = {}
    for row in rows:
      if len(vcolnames) == 1:
        values = getattr(row, vcolnames[0])
        if values is None:
          continue
      else:
        values = tuple(getattr(row, vcolname) for vcolname in vcolnames)
        if all(v is None for v in values):
          continue
      if self.support_computation_ids:
        comp_id = getattr(row, ccolname)
        if comp_id is None and gcolname is not None:
          comp_id = getattr(row, gcolname)
        results[row.entity_id] = (values, comp_id)
      else:
        results[row.entity_id] = values
    return results

  def tablesuffix(self, tablename):
    if not tablename.startswith(self.tablename_prefix):
      raise RuntimeError(f"Table name ({tablename}) does not "+\
                         f"start with prefix {self.tablename_prefix}")
    return tablename[len(self.tablename_prefix):]

  def table_attributes(self, tablename):
    return list(self._t2a[self.tablesuffix(tablename)].keys())

  def tables_for_attributes(self, attributes):
    result = {}
    for aname in attributes:
      if aname not in self._a2t:
        raise RuntimeError(f"Attribute not found: {aname}")
      tn = self.tablename(self._a2t[aname])
      if tn not in result: result[tn] = []
      result[tn].append(aname)
    return result

  def locations_for_attributes(self, attributes):
    """
    Computes the tablenames and column names where to store
    attribute values and computation IDs columns to set and to delete.

    If all elements of the group in a table are computed, the computation IDs
    to set is that of the group; in this case the computation IDs of the
    single elements must be set to NULL (to overwrite previous values, if any).

    If only part of the group is computed, the computation IDs to set
    are those of the attributes; the group ID is not set to NULL because
    it can still be valid for attributes which have not been recomputed.

    Return value:
      {"tables" =>
        {tablename => {"attrs": [...],
                       "vcols_to_set": [...],
                       "ccols_to_set": [...],
                       "ccols_to_unset": [...]},
          ...},
       "vcols" => []}
    """
    result = {"tables": {}, "vcols": []}
    for aname in attributes:
      if aname not in self._a2t:
        raise RuntimeError(f"Attribute not found: {aname}")
      t_sfx = self._a2t[aname]
      tn = self.tablename(t_sfx)
      if tn not in result["tables"]:
        result["tables"][tn] = {"vcols_to_set": [], "attrs": [],
                                "ccols_to_set": [], "ccols_to_unset": []}
      vcolnames = self._vcolnames(aname, self._t2a[t_sfx][aname])
      result["tables"][tn]["vcols_to_set"] += vcolnames
      result["tables"][tn]["attrs"].append(aname)
      result["vcols"] += vcolnames
    if self.support_computation_ids:
      for tn, tdata in result["tables"].items():
        t_sfx = self.tablesuffix(tn)
        attrs_wo_group = set(tdata["attrs"].copy())
        for g_name, g_members in self._t2g[t_sfx].items():
          attrs_wo_group -= set(g_members)
          if all(aname in tdata["attrs"] for aname in g_members):
            # whole group case
            result["tables"][tn]["ccols_to_set"].append(self._gcolname(g_name))
            result["tables"][tn]["ccols_to_unset"] += \
                [self._ccolname(aname) for aname in g_members]
          else:
            # partial group case
            result["tables"][tn]["ccols_to_set"] += \
                [self._ccolname(aname) for aname in tdata["attrs"] \
                                  if aname in g_members]
        # no group case
        for aname in attrs_wo_group:
          result["tables"][tn]["ccols_to_set"].append(self._ccolname(aname))
    return result

  def set_attribute(self, attribute, values_for_entity_ids,
                    computation_id = None):
    """
    Set attribute values for a list of entity ids
    """
    self.set_attributes([attribute], values_for_entity_ids, computation_id)

  def unset_attribute(self, attribute, entity_ids):
    """
    Unset attribute values for a list of entity ids
    """
    self.set_attributes([attribute], {eid: None for eid in entity_ids}, None)

  def set_attributes(self, attributes, values_for_entity_ids,
                     computation_id = None):
    """
    Set values of multiple attributes for a list of entity ids.
    Attributes is a list of attribute names.
    Values_for_entity_ids is a dictionary ``{entity_id: [attribute_values, ...]}``.
    The list of attribute values for each entity is flat and must have the
    same length as the sum of the number of values of the attributes.
    """
    session = Session(self.connectable)
    locations = self.locations_for_attributes(attributes)
    maybe_scalar = len(locations["vcols"]) == 1
    for tn, tdata in locations["tables"].items():
      klass = self.get_class_from_tablename(tn)
      for entity_id, values in values_for_entity_ids.items():
        row = session.query(klass).\
            filter(klass.entity_id == entity_id).first()
        if row is None:
          row = klass(entity_id=entity_id)
        if maybe_scalar and not isinstance(values, (list, tuple)):
          setattr(row, tdata["vcols_to_set"][0], values)
        else:
          for vcolname in tdata["vcols_to_set"]:
            if values is None:
              setattr(row, vcolname, None)
            else:
              value_index = locations["vcols"].index(vcolname)
              setattr(row, vcolname, values[value_index])
        if self.support_computation_ids and computation_id is not None:
          for ccolname in tdata["ccols_to_set"]:
            setattr(row, ccolname, computation_id)
          for ccolname in tdata["ccols_to_unset"]:
            setattr(row, ccolname, None)
        session.add(row)
    session.commit()

  def load_computation(self, computation_id, attributes, inputfile,
                       tmpsfx = "temporary"):
    """
    Loads data into a temporary table using LOAD DATA, then updates the
    attributes tables with the data and deletes the temporary table.
    """
    if tmpsfx in self._t2a:
      raise RuntimeError("Cannot create temporary table using "+\
                         f"tmpsfx = '{tmpsfx}' as it already exist")
    for name in attributes:
      if name not in self._a2t:
        raise RuntimeError(f"Attribute {name} does not exist")
    tmpname = self.tablename(tmpsfx)
    if tmpname in Base.metadata.tables:
      tmptable = Base.metadata.tables[tmpname]
    else:
      tmpklass = type(self.tablename(tmpsfx), (AttributeValueMixin, Base),
                      {"__tablenamepfx__": self.tablename_prefix,
                       "__tablenamesfx__": tmpsfx,
                       "__entity_id_type__": self.entity_id_type})
      tmpname = tmpklass.__tablename__
      tmptable = tmpklass.metadata.tables[tmpname]
    tmptable.create(self.connectable)
    with Session(self.connectable) as session:
      coldefs = []
      for name in attributes:
        adef = session.get(self.attrdef_class, name)
        a_datatypes = self._parse_datatype_def(adef.datatype)
        coldefs += self._vcoldefs(name, a_datatypes)
      session.execute(text(f"ALTER TABLE {tmpname} "+\
                           f"ADD COLUMN ({self._coldefstr(coldefs)})"))
      session.execute(text(f"LOAD DATA LOCAL INFILE '{inputfile}' "+\
                           f"INTO TABLE {tmpname}"))
      locations = self.locations_for_attributes(attributes)
      for tablename, tabledata in locations["tables"].items():
        columns = ["entity_id"]
        columns += [cn if cn in tabledata["vcols_to_set"] else "@dummy" \
                    for cn in locations["vcols"]]
        columns_str ="("+",".join(columns)+") "
        session.execute(text(f"LOAD DATA LOCAL INFILE '{inputfile}' "+\
                             f"IGNORE INTO TABLE {tablename}"+\
                             f"{columns_str}"))
        colsets = [(f"{tablename}.{col}", f"{tmpname}.{col}") \
                     for col in tabledata["vcols_to_set"]]
        colsets += [(f"{tablename}.{col}", ":computation_id") \
                     for col in tabledata["ccols_to_set"]]
        colsets += [(f"{tablename}.{col}", "NULL") \
                     for col in tabledata["ccols_to_unset"]]
        colsets_str = ", ".join([f"{a} = {b}" for a, b in colsets])
        session.execute(text(f"UPDATE {tablename} INNER JOIN {tmpname} "+\
                             f"USING(entity_id) SET {colsets_str}"),
                             {"computation_id":computation_id})
      session.commit()
    tmptable.drop(self.connectable)

  def _drop_table(self, sfx):
    """
    Drop table with suffix <sfx>
    """
    sfx = self.normalize_suffix(sfx)
    if sfx not in self._t2a:
      raise RuntimeError(f"Cannot drop table: no table has suffix {sfx}")
    klass = self.get_class(sfx)
    klass.metadata.tables[klass.__tablename__].drop(self.connectable)
    del self._t2a[sfx]
    del self._t2g[sfx]
    del self._ncols[sfx]

  def _drop_temporary(self, tmpsfx = "temporary"):
    with Session(self.connectable) as session:
      tmpname = self.tablename(tmpsfx)
      session.execute(f"DROP TABLE IF EXISTS {tmpname}")
      session.commit()

  def drop_all(self, tmpsfx = "temporary"):
    if self._t2a:
      sfx = list(self._t2a.keys())[0]
      klass = self.get_class(sfx)
      klass.metadata.drop_all(self.connectable)
    self._drop_temporary(tmpsfx)
    self.attrdef_class.metadata.drop_all()

  def create_table(self, sfx):
    """
    Create new table with suffix <sfx>
    """
    sfx = self.normalize_suffix(sfx)
    if sfx in self._t2a:
      raise RuntimeError(f"Cannot create table: suffix {sfx} is not unique")
    klass = type(self.tablename(sfx), (AttributeValueMixin, Base),
                 {"__tablenamepfx__": self.tablename_prefix,
                  "__tablenamesfx__": sfx,
                  "__entity_id_type__": self.entity_id_type})
    klass.metadata.tables[klass.__tablename__].create(self.connectable)
    self._t2a[sfx] = Counter()
    self._t2g[sfx] = {}
    self._ncols[sfx] = 1

  def get_class_from_tablename(self, tn):
    """
    Class reflecting table with full tablename <tn>
    """
    if not hasattr(self, "_base") or tn not in self._base.classes:
      metadata = MetaData(bind=self.connectable)
      metadata.reflect()
      self._base = automap_base(metadata=metadata)
      self._base.prepare()
    return self._base.classes[tn]

  def get_class(self, sfx):
    """
    Class reflecting table with suffix <sfx>
    """
    tn = self.tablename(self.normalize_suffix(sfx))
    return self.get_class_from_tablename(tn)

  def new_suffix(self) -> str:
    """
    Get a not-yet used numerical suffix
    """
    i = 0
    while True:
      if str(i) not in self._t2a:
        return str(i)
      i += 1

  def _place_for_new_attr(self, ncols, computation_group):
    """
    Table suffix for a new attribute for which ncols value columns and
    a computation id column are needed. Creates the table if needed.

    A new table is created if all tables have already value columns
    and the sum of the existing and new columns is higher than
    the target number of columns
    """
    for sfx in self.table_suffixes:
      if self._ncols[sfx] == 1:
        return sfx
      needed = ncols
      if self.support_computation_ids:
        needed += 1
      if self.support_computation_groups and \
           computation_group and computation_group not in self._t2g[sfx]:
        needed += 1
      if self._ncols[sfx] + needed <= self.target_n_columns:
        return sfx
    sfx = self.new_suffix()
    self.create_table(sfx)
    return sfx

  def _vcolnames(self, a_name, nelems):
    if nelems == 1: return [f"{a_name}{self.VALUE_COLUMN_SUFFIX}"]
    else:           return [f"{a_name}{self.VALUE_COLUMN_SUFFIX}{i}" \
                            for i in range(nelems)]

  def _vcoldefs(self, a_name, a_datatypes):
    return [(cn, dt) for cn, dt in zip(\
        self._vcolnames(a_name, len(a_datatypes)), a_datatypes)]

  @staticmethod
  def _coldefstr(coldefs):
    return ",".join([f"{n} {dt}" for n, dt in coldefs])

  def _ccolname(self, a_name):
    return a_name+self.COMPUTATION_COLUMN_SUFFIX

  def _gcolname(self, grp_name):
    return grp_name+self.COMPUTATION_GROUP_COLUMN_SUFFIX

  @staticmethod
  def _parse_datatype_def(datatype_def):
    result = []
    for elem in datatype_def.split(";"):
      if elem.endswith("]"):
        dts, n = elem.split("[")
        n = int(n[:-1])
        assert(n > 1)
      else:
        dts = elem
        n = 1
      if dts.endswith(")"):
        dts_type, dts_sfx = dts.split("(")
        dts_params = [ast.literal_eval(p.strip()) \
                        for p in dts_sfx[:-1].split(",")]
      else:
        dts_type = dts
        dts_params = []
      if not hasattr(sqlalchemy.types, dts_type):
        raise ValueError("Unknown datatype in attribute datatype "+\
                         f"definition {dts_type}")
      dt = getattr(sqlalchemy.types, dts_type)(*dts_params)
      result += [dt]*n
    return result

  def create_attribute(self, name, datatype, computation_group=None,
                       **kwargs):
    """
    Create a new attribute record in the attribute_definition table
    and reserve space in the attribute_values tables for storing
    the attribute values and computation IDs.

    Datatype definition, one of:
    - scalar: datatype, including any parameter in ()
    - array: datatype, followed by [<n>], with n integer > 0
    - list of scalar and/or array, semicolon-sep, wo spaces

    Datatype is thereby any of the types defined
    in the sqlAlchemy.types module.

    e.g. Boolean[8];Integer;String(12);BINARY(16)[2]
    """
    if name in self._a2t:
      raise RuntimeError(f"Attribute {name} exists already, in table number "+\
                         self._a2t[name])
    if self.support_computation_groups and computation_group is not None:
      kwargs["computation_group"] = computation_group
    adef = self.attrdef_class(name = name, datatype = datatype,
                              **kwargs)
    a_datatypes = self._parse_datatype_def(datatype)
    t_sfx = self._place_for_new_attr(len(a_datatypes), computation_group)
    tn = self.tablename(t_sfx)
    coldefs = self._vcoldefs(name, a_datatypes)
    if self.support_computation_ids:
      coldefs.append((self._ccolname(name), self.computation_id_type))
    if self.support_computation_groups and computation_group:
      if computation_group not in self._t2g[t_sfx]:
        coldefs.append((self._gcolname(computation_group),
                        self.computation_id_type))
        self._t2g[t_sfx][computation_group] = set()
      self._t2g[t_sfx][computation_group].add(name)
    with Session(self.connectable) as session:
      session.add(adef)
      session.execute(text(f"ALTER TABLE {tn} "+\
                           f"ADD COLUMN ({self._coldefstr(coldefs)})"))
      session.commit()
    self._t2a[t_sfx][name] = len(a_datatypes)
    self._a2t[name] = t_sfx
    self._ncols[t_sfx] += len(coldefs)

  def destroy_attribute(self, name):
    """
    Drop the columns for the attribute with given name and delete
    the attribute definition row.
    """
    if name not in self._a2t:
      raise RuntimeError(f"Attribute {name} does not exist")
    with Session(self.connectable) as session:
      adef = session.get(self.attrdef_class, name)
      ncols = len(self._parse_datatype_def(adef.datatype))
      t_sfx = self._a2t[name]
      colnames = self._vcolnames(name, ncols)
      if self.support_computation_ids:
        colnames.append(self._ccolname(name))
      if self.support_computation_groups:
        grp = adef.computation_group
        if grp:
          self._t2g[t_sfx][grp].remove(name)
          if len(self._t2g[t_sfx][grp]) == 0:
            del self._t2g[t_sfx][grp]
            colnames.append(self._gcolname(grp))
      dstr = ", ".join([f"DROP COLUMN {cn}" for cn in colnames])
      tn = self.tablename(t_sfx)
      session.execute(text(f"ALTER TABLE {tn} {dstr}"))
      del self._t2a[t_sfx][name]
      del self._a2t[name]
      self._ncols[t_sfx] -= len(colnames)
      session.delete(adef)
      session.commit()

  @staticmethod
  def _check_column(k, edt, cols, desc):
    if not k in cols:
      raise ValueError(f"Missing column {k} ({desc})")
    dt = cols[k]["type"]
    if not str(dt).startswith(str(edt)):
      if str(edt) == "BOOLEAN" and str(dt) == "TINYINT":
        return
      raise ValueError(f"Wrong datatype for column {k} ({desc}): "+\
                       f"found {dt}, expected {edt}")

  def check_consistency(self):
    with Session(self.connectable) as session:
      inspector = inspect(self.connectable)
      self._init_attributes_maps(inspector, session)
      unexpected_adef = session.execute(select(self.attrdef_class).where(
                        self.attrdef_class.name.notin_(
                          self.attribute_names))).all()
      if unexpected_adef:
        raise ValueError("Some attribute definitions do not correspond to "+
                         "columns in the attribute_value tables:\n"+
                         f"{unexpected_adef}")
      for sfx in self.table_suffixes:
        cols = {c["name"]: c for \
            c in inspector.get_columns(self.tablename(sfx))}
        for aname in self._t2a[sfx]:
          adef = session.execute(\
            select(self.attrdef_class).\
              where(self.attrdef_class.name==aname)).scalars().one()
          if self.support_computation_ids:
            self._check_column(self._ccolname(aname),
                               self.computation_id_type, cols,
                          f"computation ID of attribute {aname}")
          datatypes = self._parse_datatype_def(adef.datatype)
          if self.support_computation_groups:
            if adef.computation_group:
              if adef.computation_group not in self._t2g[sfx]:
                raise RuntimeError("Column for computation group "+\
                    f"{adef.computation_group} of attribute {aname} "+\
                    f"not found in table {self.tablename(sfx)}")
          if len(datatypes) == 1:
            self._check_column(self._vcolnames(aname, 1)[0], datatypes[0], cols,
                          f"value column of attribute {aname}")
          else:
            vcolnames = self._vcolnames(aname, len(datatypes))
            for i, edt in enumerate(datatypes):
              self._check_column(vcolnames[i], edt, cols,
                            f"{i} element of value of attribute {aname}")
        for gname, group_members in self._t2g[sfx].items():
          if len(group_members) == 0:
            raise RuntimeError(f"Computation ID column for group {gname} "+\
                f"found in table {self.tablename(sfx)}, but no attribute "+\
                "of this group in the table.")
          self._check_column(self._gcolname(gname),
                        self.computation_id_type, cols,
                        f"computation ID of attribute group {gname}")
