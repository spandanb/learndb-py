"""
This is a stub for "stress" tests, which will perform
a large number of random operations or perform them
for a fixed amount of time.

These should compliment, static unit tests, in that they
should run non-deterministically, and thus expose issues
that unit-tests can't catch.
"""
import logging
import itertools
import math

from .constants import EXIT_SUCCESS


def run_add_del_stress_test(db, insert_keys, del_keys):
    """
    perform some ops/validations

    :param db:
    :param insert_keys:
    :param del_keys:
    :return:
    """

    db.nuke_dbfile()

    print(f"running test case: {insert_keys} {del_keys}")

    # random.shuffle(del_keys)
    cmd = "create table foo ( colA integer primary key, colB text)"
    logging.info(f"handling [{cmd}]")
    resp = db.handle_input(cmd)

    # insert
    for key in insert_keys:
        cmd = f"insert into foo (colA, colB) values ({key}, 'hellew words foo')"
        logging.info(f"handling [{cmd}]")
        resp = db.handle_input(cmd)

    logging.debug("printing tree.......................")
    db.state_manager.print_tree("foo")

    # delete and validate
    for idx, key in enumerate(del_keys):
        # cmd = f"delete from foo where colA = {key} AND colB = 'foo'"
        cmd = f"delete from foo where colA = {key}"
        logging.info(f"handling [{cmd}]")
        resp = db.handle_input(cmd)
        if not resp.success:
            print(f"cmd {cmd} failed with {resp.status} {resp.error_message}")
            return EXIT_SUCCESS

        resp = db.handle_input("select cola, colb from foo")
        assert resp.success

        # output pipe
        pipe = db.get_pipe()

        result_keys = []
        # print anything in the output buffer
        logging.debug(f"pipe has msgs: {pipe.has_msgs()}")
        while pipe.has_msgs():
            record = pipe.read()
            key = record.get("cola")
            print(f"pipe read: {record}")
            result_keys.append(key)

        # assert result_keys == [k for k in sorted(keys)], f"result {result_keys} doesn't not match {[k for k in sorted(keys)]}"

        logging.debug("printing tree.......................")
        db.state_manager.print_tree("foo")
        # ensure tree is valid
        db.state_manager.validate_tree("foo")

        # check if all keys we expect are there in result
        expected = [key for key in sorted(del_keys[idx + 1 :])]
        actual = [key for key in sorted(set(result_keys))]
        assert actual == expected, f"expected: {expected}; received {actual}"

        print("*" * 100)

    db.close()


def run_add_del_stress_suite(learndb):
    """
    Perform a large number of add/del operation
    and validate btree correctness.
    :return:
    """

    test_cases = [
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
        [
            114,
            464,
            55,
            450,
            729,
            646,
            95,
            649,
            59,
            412,
            546,
            340,
            667,
            274,
            477,
            363,
            333,
            897,
            772,
            508,
            182,
            305,
            428,
            180,
            22,
        ],
        [15, 382, 653, 668, 139, 70, 828, 17, 891, 121, 175, 642, 491, 281, 920],
        [
            967,
            163,
            791,
            938,
            939,
            196,
            104,
            465,
            886,
            355,
            58,
            251,
            928,
            758,
            535,
            737,
            357,
            125,
            171,
            838,
            572,
            745,
            999,
            417,
            393,
            458,
            292,
            904,
            158,
            286,
            900,
            859,
            668,
            183,
        ],
        [
            726,
            361,
            583,
            121,
            908,
            789,
            842,
            67,
            871,
            461,
            522,
            394,
            225,
            637,
            792,
            393,
            656,
            748,
            39,
            696,
        ],
        [
            54,
            142,
            440,
            783,
            619,
            273,
            95,
            961,
            692,
            369,
            447,
            825,
            555,
            908,
            483,
            356,
            40,
            110,
            519,
            599,
        ],
        [
            413,
            748,
            452,
            666,
            956,
            926,
            94,
            813,
            245,
            237,
            264,
            709,
            706,
            872,
            535,
            214,
            561,
            882,
            646,
        ],
    ]

    # stress
    for test_case in test_cases:
        insert_keys = test_case
        # del_keys = test_case[:]

        # there is a large number of perms ~O(n!)
        # and they are generated in a predictable order
        # we'll skip based on fixed step- later, this too should be randomized
        num_perms = 1
        total_perms = math.factorial(len(insert_keys))
        del_perms = []

        step_size = min(total_perms // num_perms, 10)
        # iterator over permutations
        perm_iter = itertools.permutations(insert_keys)

        while len(del_perms) < num_perms:
            for _ in range(step_size - 1):
                # skip n-1 deletes
                next(perm_iter)
            del_perms.append(next(perm_iter))

        for del_keys in del_perms:
            try:
                run_add_del_stress_test(learndb, insert_keys, del_keys)
            except Exception as e:
                logging.error(
                    f"Inner devloop failed on: {insert_keys} {del_keys} with {e}"
                )
                raise
