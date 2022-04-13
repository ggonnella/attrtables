#
# (c) 2022 Giorgio Gonnella, University of Goettingen, Germany
#
"""
Mixin for tables used for storing attribute information.
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy_repr import PrettyRepresentableBase
from sqlalchemy.orm import declared_attr

utf8_cs_args = {'mysql_charset': 'utf8', 'mysql_collate': 'utf8_bin'}

Base = declarative_base(cls=PrettyRepresentableBase)

class AttributeDefinition(Base):
  __tablename__ = "attribute_definition"
  name = Column(String(62), primary_key=True)
  datatype = Column(String(256), nullable=False)
  computation_group = Column(String(62), index=True)
  __table_args__ = utf8_cs_args

