#
# (c) 2022 Giorgio Gonnella, University of Goettingen, Germany
#
import pytest
from collections import defaultdict
import os
from sqlalchemy import create_engine, inspect, exc
from sqlalchemy.orm import Session
from sqlalchemy.engine.url import URL
import yaml

VERBOSE_CONNECTION = False

@pytest.fixture(scope="session")
def connection_string():
  # if config.yaml does not exist, raise an error
  config_file_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
  if not os.path.exists(config_file_path):
    raise FileNotFoundError(\
        'Connection configuration file config.yaml not found')
  with open(config_file_path) as f:
    config = yaml.safe_load(f)
  args = {k: v for k, v in config.items() if k in ['drivername',
                                           'host', 'port', 'database',
                                           'username', 'password']}
  if 'socket' in config:
    args['query'] = {'unix_socket': config['socket']}
  return URL.create(**args)

@pytest.fixture(scope="session")
def connection(connection_string):
  engine = create_engine(connection_string, echo=VERBOSE_CONNECTION,
                         future=True)
  with engine.connect() as conn:
    with conn.begin():
      yield conn
      conn.commit()
