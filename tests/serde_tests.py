"""
Tests serde of individual datatypes and of schemas/records
composed of columns of many different datatype
"""
from .context import (REAL_EPSILON, datatypes, SimpleSchema, Column, SimpleRecord, deserialize_cell,
                      serialize_record)


def test_integer_serde():
    values = [4, 100, 109297]
    for value in values:
        # create int
        datatype = datatypes.Integer
        # serialize int
        ser_val = datatype.serialize(value)
        # deserialize bytes
        deser_val = datatype.deserialize(ser_val)
        # assert initial value equals round-tripped value
        assert deser_val == value


def test_real_serde():
    values = [4.0, 11.7, 19.297]
    for value in values:
        # create int
        datatype = datatypes.Real
        # serialize int
        ser_val = datatype.serialize(value)
        # deserialize bytes
        deser_val = datatype.deserialize(ser_val)
        # assert initial value approximately equals round-tripped value
        # NOTE: since floats don't convert exactly, we need to
        # compare value given float representation limitations.
        # not sure if it's valid to compare the diff of values
        # be less than threshold- since the threshold may vary depending on magnitude?
        assert abs(deser_val - value) < REAL_EPSILON


def test_key_only_schema_serde():
    """
    Attempt to serialize and deserialize a schema
    :return:
    """
    schema = SimpleSchema('dummy', [
            Column('pkey', datatypes.Integer, is_primary_key=True)
        ])
    # create a record that matches above schema
    record = SimpleRecord({"pkey": 1}, schema)

    # serialize
    resp = serialize_record(record)
    assert resp.success, "serialize failed"
    serialized = resp.body
    # deserialize
    resp = deserialize_cell(serialized, schema)
    assert resp.success, "deserialize failed"
    deserialized = resp.body

    # validate original and deserialized record have the same value
    for col in schema.columns:
        assert record.values[col.name] == deserialized.values[col.name]


def test_multi_column_fixed_len_type_serde():
    """
    Attempt to serialize and deserialize a schema
    :return:
    """
    schema = SimpleSchema('dummy', [
            Column('pkey', datatypes.Integer, is_primary_key=True),
            Column('name', datatypes.Text),
            Column('root_pagenum', datatypes.Integer)
        ])
    # create a record that matches above schema
    record = SimpleRecord({"pkey": 1, "name": "some_table_nane", "root_pagenum": 2}, schema)

    # serialize
    resp = serialize_record(record)
    assert resp.success, "serialize failed"
    serialized = resp.body
    # deserialize
    resp = deserialize_cell(serialized, schema)
    assert resp.success, "deserialize failed"
    deserialized = resp.body

    # validate original and deserialized record have the same value
    for col in schema.columns:
        assert record.values[col.name] == deserialized.values[col.name]


def test_nullable_serde():
    """
    Attempt to serialize and deserialize a schema
    :return:
    """
    schema = SimpleSchema('dummy', [
            Column('pkey', datatypes.Integer, is_primary_key=True),
            Column('name', datatypes.Text),
            Column('root_pagenum', datatypes.Integer),
            Column('sql', datatypes.Text)
        ])
    # create a record that matches above schema
    record = SimpleRecord({"pkey": 1, "name": "some_table_nane", "root_pagenum": 2, "sql": None}, schema)

    # serialize
    resp = serialize_record(record)
    assert resp.success, "serialize failed"
    serialized = resp.body
    # deserialize
    resp = deserialize_cell(serialized, schema)
    assert resp.success, "deserialize failed"
    deserialized = resp.body

    # validate original and deserialized record have the same value
    for col in schema.columns:
        assert record.values[col.name] == deserialized.values[col.name]


