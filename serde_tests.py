"""
Tests serde of individual datatypes and of schemas/records
composed of columns of many different datatype
"""
from datatypes import Integer, Float, Text, Null, Blob
from schema import Schema, Column, Record
from serde import deserialize_cell, serialize_record


def test_integer_serde():
    values = [4, 100, 109297]
    for value in values:
        # create int
        datatype = Integer
        # serialize int
        ser_val = datatype.serialize(value)
        # deserialize bytes
        deser_val = datatype.deserialize(ser_val)
        # assert initial value equals round-tripped value
        assert deser_val == value


def test_float_serde():
    values = [4.0, 11.7, 19.297]
    for value in values:
        # create int
        datatype = Float
        # serialize int
        ser_val = datatype.serialize(value)
        # deserialize bytes
        deser_val = datatype.deserialize(ser_val)
        # assert initial value approximately equals round-tripped value
        # NOTE: since floats don't convert exactly, we need to
        # compare value given float representation limitations.
        # not sure if it's valid to compare the diff of values
        # be less than threshold- since the threshold may vary depending on magnitude?
        assert abs(deser_val - value) < 0.001


def test_schema_serde():
    """
    Attempt to serialize and deserialize a schema
    :return:
    """
    schema = Schema('dummy', [
            Column('pkey', Integer, is_primary_key=True),
            Column('name', Text),
            Column('root_pagenum', Integer),
            Column('sql', Text)
        ])
    # create a record that matches above schema
    record = Record({"pkey": 1, "name": "some_table_nane", "root_pagenum": 2}, schema)

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



