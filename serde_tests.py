"""
Tests various aspects of schema
"""
from datatypes import Integer, Float, Text, Null, Blob


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