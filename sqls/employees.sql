create table employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    salary INTEGER,
    depid INTEGER
);

INSERT INTO employees(id, name, salary, depid) VALUES (1, 'John', 100, 1);
INSERT INTO employees(id, name, salary, depid) VALUES (2, 'Anita', 200, 1);
INSERT INTO employees(id, name, salary, depid) VALUES (3, 'Gab', 100, 2);

create table department (
    depid INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO department(depid, name) VALUES (1, 'accounting');
INSERT INTO department(depid, name) VALUES (2, 'sales');
INSERT INTO department(depid, name) VALUES (3, 'engineering');

select e.name, d.name from employees e inner join department d on e.depid = d.depid;

select count(e.name), d.depid from employees e inner join department d on e.depid = d.depid group by d.depid;

select count(e.name), d.depid from  department d left join employees e on e.depid = d.depid group by d.depid;

select count(e.name), d.depid from employees e right join department d on e.depid = d.depid group by d.depid;