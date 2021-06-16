import os

from tempfile import NamedTemporaryFile
from learndb import db_open, insert_helper, input_handler


DB_FILE = "db.test.file"


def test_cases():
    # list of test cases
    cases = [
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
        [114, 464, 55, 450, 729, 646, 95, 649, 59, 412, 546, 340, 667, 274, 477, 363, 333, 897, 772, 508, 182, 305, 428, 180, 22],
        [15, 382, 653, 668, 139, 70, 828, 17, 891, 121, 175, 642, 491, 281, 920],
        [967, 163, 791, 938, 939, 196, 104, 465, 886, 355, 58, 251, 928, 758, 535, 737, 357, 125, 171, 58, 838, 572, 745, 999, 417, 393, 458, 292, 904, 158, 286, 900, 859, 668, 183],
        [726, 361, 583, 121, 908, 789, 842, 67, 871, 461, 522, 394, 225, 637, 792, 393, 656, 748, 39, 696],
        [54, 142, 440, 783, 619, 273, 95, 961, 692, 369, 447, 825, 555, 908, 483, 356, 40, 110, 519, 599],
        [413, 748, 452, 666, 956, 926, 94, 813, 245, 237, 264, 709, 706, 872, 706, 535, 214, 561, 882, 646]
    ]

    # failure should raise an assertion
    for test_case in cases:
        # recreate db, before each test
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        # create table
        table = db_open(DB_FILE)

        print(f"running: {test_case}")
        multi_insert(test_case, table)
        check_existence(test_case, table)

        del table


def check_existence(expected: list, table):
    """
    check whether each of the keys in `expected` exists in database
    """
    table.tree.validate_existence(expected)


def multi_insert(values: list, table):
    """
    This tests inserts all `values` into the
    database and validates the tree after the operation
    :param values:
    :return:
    """

    for value in values:
        insert_helper(table, value)
        input_handler('.validate', table)

    # input_handler('select', table)
    # input_handler('.btree', table)
    # input_handler('.quit', table)
