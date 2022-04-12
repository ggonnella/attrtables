#!/usr/bin/env python3
"""
Example of table for storing attribute information.
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy_repr import PrettyRepresentableBase
from sqlalchemy.orm import declared_attr

Base = declarative_base(cls=PrettyRepresentableBase)
utf8_cs_args = {'mysql_charset': 'utf8', 'mysql_collate': 'utf8_bin'}

class AttributeDefinition(Base):
  name = Column(String(62), primary_key=True)
  datatype = Column(String(256), nullable=False)
  computation_group = Column(String(62), index=True)
  __table_args__ = utf8_cs_args

  @declared_attr
  def __tablename__(cls):
    return str(cls.__set_tablename__)

