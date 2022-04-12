#!/usr/bin/env python3
import pytest
from collections import defaultdict
import os
import uuid
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session
from sqlalchemy.engine.url import URL
from attrtables.attribute_value_tables import AttributeValueTables

def test_scalar_attributes(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass)
  for atype in ["Integer", "Float", "String(50)", "Text", "Boolean"]:
    aname = atype[0].lower()
    avt.create_attribute(aname, atype)
    avt.check_consistency()
    klass, vcols, ccol, gcol = avt.attribute_access_data(aname)
    assert(str(klass.__name__) == "attribute_value_t0")
    assert(vcols == [f"{aname}_v"])
    assert(ccol == f"{aname}_c")
    assert(gcol == None)
    avt.destroy_attribute(aname)

def test_multiscalar_attributes(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass)
  for atype in ["Integer;Float", "String(50);Text", "Boolean;Integer;Float"]:
    aname = atype[0].lower()
    avt.create_attribute(aname, atype)
    avt.check_consistency()
    klass, vcols, ccol, gcol = avt.attribute_access_data(aname)
    assert(str(klass.__name__) == "attribute_value_t0")
    assert(vcols == [f"{aname}_v{i}" for i in range(len(atype.split(";")))])
    assert(ccol == f"{aname}_c")
    assert(gcol == None)
    avt.destroy_attribute(aname)

def test_array_attributes(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass)
  for atype, anum in [("Integer[10]", 10),
                      ("Boolean;String(50)[3];Integer;Text[3];Float", 9)]:
    aname = atype[0].lower()
    avt.create_attribute(aname, atype)
    avt.check_consistency()
    klass, vcols, ccol, gcol = avt.attribute_access_data(aname)
    assert(str(klass.__name__) == "attribute_value_t0")
    assert(vcols == [f"{aname}_v{i}" for i in range(anum)])
    assert(ccol == f"{aname}_c")
    assert(gcol == None)
    avt.destroy_attribute(aname)

def test_create_computation_group(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass)
  atypes = ["Integer", "Float", "String(50)", "Text", "Boolean"]
  for atype in atypes:
    aname = atype[0].lower()
    avt.create_attribute(aname, atype, computation_group="cgrp")
    avt.check_consistency()
  for atype in atypes:
    aname = atype[0].lower()
    klass, vcols, ccol, gcol = avt.attribute_access_data(aname)
    assert(str(klass.__name__) == "attribute_value_t0")
    assert(vcols == [f"{aname}_v"])
    assert(ccol == f"{aname}_c")
    assert(gcol == "cgrp_g")
  for atype in atypes:
    aname = atype[0].lower()
    avt.destroy_attribute(aname)

def test_attribute_destruction(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass)
  avt.create_attribute("a", "Integer")
  assert(str(avt.attribute_class("a").__name__) == "attribute_value_t0")
  attr_records = Session(connection).execute(select(attrdefclass).\
      where(attrdefclass.name == "a")).scalars().all()
  assert(len(attr_records) == 1)
  assert(attr_records[0].name == "a")
  avt.destroy_attribute("a")
  assert(avt.attribute_class("a") == None)
  attr_records = Session(connection).execute(select(attrdefclass).\
      where(attrdefclass.name == "a")).scalars().all()
  assert(len(attr_records) == 0)

def test_custom_table_prefix(connection, attrdefclass):
  try:
    avt = AttributeValueTables(connection, attrdefclass,
                              tablename_prefix="custom_tabpfx_")
    avt.create_attribute("a", "Integer")
    avt.check_consistency()
    klass = avt.attribute_class("a")
    assert(str(klass.__name__) == "custom_tabpfx_0")
  finally:
    avt.destroy_attribute("a")

def create_attributes_a_to_h(avt):
  avt.create_attribute("a", "Integer", computation_group="g1")
  avt.create_attribute("b", "Integer;Float", computation_group="g1")
  avt.create_attribute("c", "String(1)[3]")
  avt.create_attribute("d", "Integer", computation_group="g1")
  avt.create_attribute("e", "Float", computation_group="g2")
  avt.create_attribute("f", "Integer", computation_group="g2")
  avt.create_attribute("g", "Integer", computation_group="g1")
  avt.create_attribute("h", "Integer")

VALUES_A = {"e1": 1, "e3": 100}

#           group   g1  None  g1   g2   g2   g1  None
TABLE_SFX_B_TO_H = [0,   1,   0,   1,   2,   2,   2]
ATTRNAMES_B_TO_H = ["b", "c", "d", "e", "f", "g", "h"]
VALUES_B_TO_H = {\
    "e1": [2, 3.3, "4", "5", "6", 7, 8.8, 9, 10, 11],
    "e2": [20, 30.3, "X", "Y", "Z", 70, 80.8, 90, 100, 110],
    "e3": [200, 333.3, "A", "B", "C", None, 888.8, 900, 1000, 1100]}

COMPUTATION_ID1 = uuid.UUID(f'00000000-0000-0000-0000-000000000001').bytes
COMPUTATION_ID2 = uuid.UUID(f'00000000-0000-0000-0000-000000000002').bytes

def test_attr_n_columns_larger_than_target(connection, attrdefclass):
  try:
    avt = AttributeValueTables(connection, attrdefclass, target_n_columns = 3)
    avt.create_attribute("a", "Integer[10]")
    assert(avt.attribute_class("a").__name__ == "attribute_value_t0")
    assert(avt.attribute_value_columns("a") == [f"a_v{i}" for i in range(10)])
    assert(avt.attribute_computation_column("a") == f"a_c")
    assert(avt.attribute_computation_group_column("a") == None)
    avt.create_attribute("b", "Integer")
    assert(avt.attribute_class("b").__name__ == "attribute_value_t1")
    assert(avt.attribute_value_columns("b") == ["b_v"])
    assert(avt.attribute_computation_column("b") == f"b_c")
    assert(avt.attribute_computation_group_column("b") == None)
  finally:
    avt.destroy_attribute("a")
    avt.destroy_attribute("b")

def test_multiple_tables(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass, target_n_columns = 9)
  create_attributes_a_to_h(avt)
  try:
    avt.check_consistency()
    table_names = [avt.attribute_class(aname).__name__ \
                     for aname in ["a"]+ATTRNAMES_B_TO_H]
    assert(table_names == [f"attribute_value_t{t}" for t in \
                             [0]+TABLE_SFX_B_TO_H])
    vcols = [avt.attribute_value_columns(aname) \
        for aname in ["a"]+ATTRNAMES_B_TO_H]
    assert(vcols == [["a_v"], ["b_v0", "b_v1"], ["c_v0", "c_v1", "c_v2"],
                     ["d_v"], ["e_v"], ["f_v"], ["g_v"], ["h_v"]])
    ccols = [avt.attribute_computation_column(aname) \
        for aname in ["a"]+ATTRNAMES_B_TO_H]
    assert(ccols == [f"{aname}_c" for aname in ["a"]+ATTRNAMES_B_TO_H])
    gcols = [avt.attribute_computation_group_column(aname) \
        for aname in ["a"]+ATTRNAMES_B_TO_H]
    assert(gcols == ["g1_g", "g1_g", None, "g1_g", \
                     "g2_g", "g2_g", "g1_g", None])
  finally:
    for aname in ["a"] + ATTRNAMES_B_TO_H:
      avt.destroy_attribute(aname)

def test_set_and_query(connection, attrdefclass):
  avt = AttributeValueTables(connection, attrdefclass, target_n_columns = 9)
  create_attributes_a_to_h(avt)
  try:
    avt.check_consistency()
    avt.set_attribute("a", VALUES_A, COMPUTATION_ID1)
    avt.set_attribute("d", {"e3": 700}, COMPUTATION_ID1)
    results = avt.query_attribute("d", ["e1", "e2", "e3"])
    assert(results == {"e3": (700, COMPUTATION_ID1)})
    results = avt.query_attribute("a", ["e1"])
    assert(results == {"e1": (1, COMPUTATION_ID1)})
    results = avt.query_attribute("a", ["e2", "e3"])
    assert(results == {"e3": (100, COMPUTATION_ID1)})
    avt.set_attributes(ATTRNAMES_B_TO_H, VALUES_B_TO_H, COMPUTATION_ID2)
    results = avt.query_attribute("b", ["e1", "e2"])
    assert(results == {"e1": ((2, 3.3), COMPUTATION_ID2),
                       "e2": ((20, 30.3), COMPUTATION_ID2)})
    results = avt.query_attribute("c", ["e1", "e3"])
    assert(results == {"e1": (("4", "5", "6"), COMPUTATION_ID2),
                       "e3": (("A", "B", "C"), COMPUTATION_ID2)})
    results = avt.query_attribute("d", ["e1", "e2", "e3"])
    assert(results == {"e1": (7, COMPUTATION_ID2),
                       "e2": (70, COMPUTATION_ID2)})
    avt.set_attribute("d", {"e2": 70, "e3": 700}, COMPUTATION_ID1)
    results = avt.query_attribute("d", ["e1", "e2", "e3"])
    assert(results == {"e1": (7, COMPUTATION_ID2),
                       "e2": (70, COMPUTATION_ID1),
                       "e3": (700, COMPUTATION_ID1)})
    avt.unset_attribute("b", ["e1", "e2"])
    results = avt.query_attribute("b", ["e1", "e2", "e3"])
    assert(results == {"e3": ((200, 333.3), COMPUTATION_ID2)})
    results = avt.query_attribute("d", ["e1", "e2", "e3"])
    assert(results == {"e1": (7, COMPUTATION_ID2),
                       "e2": (70, COMPUTATION_ID1),
                       "e3": (700, COMPUTATION_ID1)})
  finally:
    for aname in ["a"] + ATTRNAMES_B_TO_H:
      avt.destroy_attribute(aname)

  # LOAD COMPUTATION
  # avt.load_computation(computation_id, plugin.OUTPUT, args["<results>"])
  #
  # def load_computation(self, computation_id, attributes, inputfile,
  #                    tmpsfx = "temporary"):
  #

# db_attributes to create the attributes
# db_load_results


    # still to be implemented:
    #   information reports (how many attributes, which datatypes,
    #   how many values, computation IDs, etc.)
