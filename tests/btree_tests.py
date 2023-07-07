"""
These are the new btree tests. These indirectly test the
btree functionality via the frontend. I prefer this, as this simplifies
the testing; otherwise, I'll have to import serde logic to generate formatted cells
"""
import pytest
import random

from .context import LearnDB
from .test_constants import TEST_DB_FILE


@pytest.fixture
def test_cases():
    return [
            [1, 2, 3, 4],
            [64, 5, 13, 82],
            [82, 13, 5, 2, 0],
            [10, 20, 30, 40, 50, 60, 70],
            [72, 79, 96, 38, 47],
            [432, 507, 311, 35, 246, 950, 956, 929, 769, 744, 994, 438],
            [159, 597, 520, 189, 822, 725, 504, 397, 218, 134, 516],
            [159, 597, 520, 189, 822, 725, 504, 397],
            [960, 267, 947, 400, 795, 327, 464, 884, 667, 870, 92],
            [793, 651, 165, 282, 177, 439, 593],
            [229, 653, 248, 298, 801, 947, 63, 619, 475, 422, 856, 57, 38],
            [103, 394, 484, 380, 834, 677, 604, 611, 952, 71, 568, 291, 433, 305],
            [114, 464, 55, 450, 729, 646, 95, 649, 59, 412, 546, 340, 667, 274, 477, 363, 333, 897, 772, 508, 182, 305, 428,
                180, 22],
            [15, 382, 653, 668, 139, 70, 828, 17, 891, 121, 175, 642, 491, 281, 920],
            [967, 163, 791, 938, 939, 196, 104, 465, 886, 355, 58, 251, 928, 758, 535, 737, 357, 125, 171, 838, 572, 745,
                999, 417, 393, 458, 292, 904, 158, 286, 900, 859, 668, 183],
            [726, 361, 583, 121, 908, 789, 842, 67, 871, 461, 522, 394, 225, 637, 792, 393, 656, 748, 39, 696],
            [54, 142, 440, 783, 619, 273, 95, 961, 692, 369, 447, 825, 555, 908, 483, 356, 40, 110, 519, 599],
            [413, 748, 452, 666, 956, 926, 94, 813, 245, 237, 264, 709, 706, 872, 535, 214, 561, 882, 646]
        ]


@pytest.fixture
def tiny_test_cases():
    return [
            [1, 2, 3, 4],
        ]

@pytest.fixture
def small_test_cases():
    return [
            [1, 2, 3, 4],
            [4, 3, 2, 1],
            [64, 5, 13, 82],
            [82, 13, 5, 2, 0],
            [10, 20, 30, 40, 50, 60, 70],
        ]


def test_inserts(test_cases):
    """
    iterate over test cases, insert keys
    - validate tree
    - scan table and ensure keys are sorted version of inputted keys

    :param test_cases: fixture
    :return:
    """

    for test_case in test_cases:
        db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
        # delete old file
        db.nuke_dbfile()

        # test interfaces via db frontend
        # create table before inserting
        # TODO: FIX me current parser + VM can't handle mixed-case column names, e.g.
        # colA; for now making them all lowercase
        #db.handle_input("create table foo ( colA integer primary key, colB text)")
        db.handle_input("create table foo ( cola integer primary key, colb text)")

        # insert keys
        for idx, key in enumerate(test_case):
            db.handle_input(f"insert into foo (cola, colb) values ({key}, 'hello world')")

            # select rows
            db.handle_input("select cola, colb  from foo")
            pipe = db.get_pipe()
            assert pipe.has_msgs(), "expected rows"
            # collect keys into a list
            result_keys = []
            while pipe.has_msgs():
                record = pipe.read()
                key = record.get("cola")
                result_keys.append(key)

            db.virtual_machine.state_manager.validate_tree("foo")
            sorted_test_case = [k for k in sorted(test_case[:idx+1])]
            assert result_keys == sorted_test_case, f"result {result_keys} doesn't not match {sorted_test_case}"

        db.close()
        del db


def test_deletes(test_cases):
    """
    iterate over test cases- insert all keys
    then delete keys and ensure:
    - tree is consistent
    - has expected keys

    :param test_cases:
    :return:
    """

    for test_case in test_cases:
        db = LearnDB(TEST_DB_FILE)
        # delete old file
        db.nuke_dbfile()

        # test interfaces via db frontend
        # create table before inserting
        # db.handle_input("create table foo ( cola integer primary key, colb text)")
        db.handle_input("create table foo ( cola integer primary key, colb text)")

        # insert keys
        for key in test_case:
            db.handle_input(f"insert into foo (colA, colB) values ({key}, 'hello world')")

        # shuffle keys in repeatable order
        random.seed(1)
        del_keys = test_case[:]
        random.shuffle(del_keys)

        for idx, key in enumerate(del_keys):
            try:
                # delete key
                db.handle_input(f"delete from foo where cola = {key}")
                # validate input

                # select rows
                db.handle_input("select cola, colb  from foo")
                pipe = db.get_pipe()

                # collect keys into a list
                result_keys = []
                while pipe.has_msgs():
                    record = pipe.read()
                    key = record.get("cola")
                    result_keys.append(key)

                try:
                    db.virtual_machine.state_manager.validate_tree("foo")
                except Exception as e:
                    raise Exception(f"validate tree failed for {idx} {del_keys} with {e}")
                sorted_test_case = [k for k in sorted(del_keys[idx+1:])]
                assert result_keys == sorted_test_case, f"result {result_keys} doesn't not match {sorted_test_case}"
            except Exception as e:
                raise Exception(f"Delete test case [{test_case}][{idx}] {del_keys} with {e}")

        db.close()
        del db
