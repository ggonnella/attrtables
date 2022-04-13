#
# (c) 2022 Giorgio Gonnella, University of Goettingen, Germany
#
"""
Mixin used as common base for each element of the Attribute Value tables set
"""
from sqlalchemy import Column, String
from sqlalchemy.orm import declared_attr

utf8_cs_args = {'mysql_charset': 'utf8', 'mysql_collate': 'utf8_bin'}

class AttributeValueMixin:
  __table_args__ = utf8_cs_args
  __table_args__["extend_existing"] = True
  __table_args__["autoload_replace"] = True

  @declared_attr
  def entity_id(cls):
    return Column(cls.__entity_id_type__, primary_key = True)

  @declared_attr
  def __tablename__(cls):
    """
    The tablename is computed from the common prefix and
    the suffix, which is different for each table of the tables set.
    """
    return str(cls.__tablenamepfx__) + str(cls.__tablenamesfx__)

