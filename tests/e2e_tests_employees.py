"""
Set of tests on employees schema
"""
import pytest

from .context import LearnDB
from .test_constants import TEST_DB_FILE

# utils


@pytest.fixture
def db_employees():
    """
    Return db with employees schema
    """
    commands = [
        """create table employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                salary INTEGER,
                depid INTEGER)""",

        "INSERT INTO employees(id, name, salary, depid) VALUES (1, 'John', 100, 1)",
        "INSERT INTO employees(id, name, salary, depid) VALUES (2, 'Anita', 200, 1)",
        "INSERT INTO employees(id, name, salary, depid) VALUES (3, 'Gab', 100, 2)",

        """create table department (
            depid INTEGER PRIMARY KEY,
            name TEXT)""",

        "INSERT INTO department(depid, name) VALUES (1, 'accounting')",
        "INSERT INTO department(depid, name) VALUES (2, 'sales')",
        "INSERT INTO department(depid, name) VALUES (3, 'engineering')",
    ]
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    return db


# test

def test_select_inner_join(db_employees):
    db_employees.handle_input("select e.name, d.name from employees e inner join department d on e.depid = d.depid")
    employees = {}
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        employee_name = record.at_index(0)
        dep_name = record.at_index(1)
        if dep_name not in employees:
            employees[dep_name] = set()
        employees[dep_name].add(employee_name)
    assert employees["accounting"] == {"Anita", "John"}
    assert employees["sales"] == {"Gab"}


def test_select_left_join_and_group_by(db_employees):
    """
    Count number of employees in department, even for departments with no employees
    count employees after doing department left join employees
    """
    db_employees.handle_input("select count(e.name), d.name from department d left join employees e on e.depid = d.depid group by d.name")
    employees = {}
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        employee_count = record.at_index(0)
        dep_name = record.at_index(1)
        employees[dep_name] = employee_count
    assert employees["accounting"] == 2
    assert employees["sales"] == 1
    assert employees["engineering"] == 0


def test_select_right_join_and_group_by(db_employees):
    """
    Count number of employees in department, even for departments with no employees
    count employees after doing department left join employees
    """
    db_employees.handle_input("select count(e.name), d.name from employees e right join department d on e.depid = d.depid group by d.name")
    employees = {}
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        employee_count = record.at_index(0)
        dep_name = record.at_index(1)
        employees[dep_name] = employee_count
    assert employees["accounting"] == 2
    assert employees["sales"] == 1
    assert employees["engineering"] == 0


def test_select_group_by_and_having(db_employees):
    # todo: add having test
    [

        "select e.name, d.name from employees e inner join department d on e.depid = d.depid",

        "select count(e.name), d.depid from employees e inner join department d on e.depid = d.depid group by d.depid",

        "select count(e.name), d.depid from  department d left join employees e on e.depid = d.depid group by d.depid",

        "select count(e.name), d.depid, d.name from employees e right join department d on e.depid = d.depid group by d.depid, d.name",
    ]