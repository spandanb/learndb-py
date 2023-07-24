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
        "INSERT INTO employees(id, name, salary, depid) VALUES (2, 'Anita', 300, 1)",
        "INSERT INTO employees(id, name, salary, depid) VALUES (3, 'Gab', 200, 2)",

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


@pytest.fixture
def db_fruits():
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    commands = [
        """CREATE TABLE fruits ( 
        id INTEGER PRIMARY KEY, 
        name TEXT, 
        avg_weight INTEGER)
        """,
        "insert into fruits (id, name, avg_weight) values (1, 'apple', 200)",
        "insert into fruits (id, name, avg_weight) values (2, 'orange', 140)",
        "insert into fruits (id, name, avg_weight) values (3, 'pineapple', 1000)",
        "insert into fruits (id, name, avg_weight) values (4, 'grape', 5)",
        "insert into fruits (id, name, avg_weight) values (5, 'pear', 166)",
        "insert into fruits (id, name, avg_weight) values (6, 'mango', 140)",
        "insert into fruits (id, name, avg_weight) values (7, 'watermelon', 10000)",
        "insert into fruits (id, name, avg_weight) values (8, 'banana', 118)",
        "insert into fruits (id, name, avg_weight) values (9, 'peach', 147)",
    ]
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
    db_employees.handle_input("select count(e.name), d.name from employees e inner join department d on e.depid = d.depid group by d.name having count(e.name) < 2")
    employees = {}
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        employee_count = record.at_index(0)
        dep_name = record.at_index(1)
        employees[dep_name] = employee_count
    assert len(employees) == 1
    assert employees["sales"] == 1


def test_order_limit(db_fruits):
    db_fruits.handle_input("select name, id from fruits order by id limit 5")
    values = []
    while db_fruits.get_pipe().has_msgs():
        record = db_fruits.get_pipe().read()
        fruit = record.at_index(0)
        values.append(fruit)
    expected = ['apple', 'orange', 'pineapple', 'grape', 'pear']
    assert expected == values


def test_multi_column_order(db_fruits):
    db_fruits.handle_input("select name, avg_weight from fruits order by avg_weight, name desc limit 4")
    values = []
    while db_fruits.get_pipe().has_msgs():
        record = db_fruits.get_pipe().read()
        fruit = record.at_index(0)
        values.append(fruit)
    # critically, mango and orange have the same weight
    # descending ordering on name, means mango does first
    expected = ['grape', 'banana', 'orange', 'mango']
    assert expected == values


def test_table_drop(db_employees):
    db_employees.handle_input("SELECT name from catalog")
    table_names = []
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        table_name = record.at_index(0)
        table_names.append(table_name)
    assert len(table_names) == 2
    assert "department" in table_names and "employees" in table_names

    db_employees.handle_input("DROP TABLE employees")
    db_employees.handle_input("SELECT name from catalog")
    table_names = []
    while db_employees.get_pipe().has_msgs():
        record = db_employees.get_pipe().read()
        table_name = record.at_index(0)
        table_names.append(table_name)
    assert len(table_names) == 1
    assert table_names[0] == "department"
