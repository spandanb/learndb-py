"""
This sets up the modules for testing
"""
import os
import sys
# otherwise everything that needs to be tested will have to be explicitly imported
# which would make the top level export expose items that aren't intended for user access
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# specific internal imports for specific tests suites
# generally we'll import entire module, unless it' clearer to import a specific member

from learndb.constants import REAL_EPSILON

# learndb
from learndb.interface import LearnDB

# lang_tests
from learndb.lang_parser.sqlhandler import SqlFrontEnd

from learndb import datatypes
from learndb.schema import SimpleSchema, Column
from learndb.record_utils import SimpleRecord
from learndb.serde import deserialize_cell, serialize_record

from learndb.pager import Pager