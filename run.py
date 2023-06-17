"""
Main interface for user/developer of learndb.

Utility to start repl and run commands.

Requires learndb to be installed.
"""

import sys

from learndb import parse_args_and_start


if __name__ == '__main__':
    parse_args_and_start(sys.argv[1:])