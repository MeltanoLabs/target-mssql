"""Unit tests for target_mssql.arrow_bulkcopy (no live DB required)."""

# ruff: noqa: S101, S105

from __future__ import annotations

import pyarrow as pa
import pytest
from singer_sdk.singerlib.json import deserialize_json

from target_mssql import arrow_bulkcopy
from target_mssql.sinks import MSSQLSink
from target_mssql.target import TargetMSSQL


def test_connect_kwargs_from_url_discrete_fields():
    kwargs = arrow_bulkcopy.connect_kwargs_from_url(
        "mssql+pymssql://sa:P%4055w0rd@localhost:1433/master",
        trust_server_certificate=False,
    )
    assert kwargs["Server"] == "localhost,1433"
    assert kwargs["Database"] == "master"
    assert kwargs["UID"] == "sa"
    assert kwargs["PWD"] == "P@55w0rd"
    assert "TrustServerCertificate" not in kwargs


def test_connect_kwargs_from_url_trust_server_certificate():
    kwargs = arrow_bulkcopy.connect_kwargs_from_url(
        "mssql+pyodbc://sa:pw@myhost:1433/mydb",
        trust_server_certificate=True,
    )
    assert kwargs["Server"] == "myhost,1433"
    assert kwargs["TrustServerCertificate"] == "yes"


def test_connect_kwargs_from_url_no_port():
    kwargs = arrow_bulkcopy.connect_kwargs_from_url("mssql+pymssql://sa:pw@myhost/mydb")
    assert kwargs["Server"] == "myhost"


PAYLOAD = '{"route": "Guardian", "score": 1.5}'


@pytest.mark.parametrize(
    ("arrow_values", "record_value", "property_type"),
    [
        pytest.param(pa.array([PAYLOAD], type=pa.json_()), PAYLOAD, "object", id="top-level"),
        pytest.param(
            pa.StructArray.from_arrays([pa.array([PAYLOAD], type=pa.json_())], names=["data"]),
            f'{{"data": {PAYLOAD}}}',
            "object",
            id="struct-field",
        ),
        pytest.param(
            pa.ListArray.from_arrays([0, 2], pa.array([PAYLOAD, "[1, 2]"], type=pa.json_())),
            f"[{PAYLOAD}, [1, 2]]",
            "array",
            id="list-items",
        ),
    ],
)
def test_arrow_json_matches_record(arrow_values, record_value, property_type):
    schema = {"properties": {"col": {"type": [property_type, "null"]}}}
    target = TargetMSSQL(
        config={"username": "sa", "password": "pw", "host": "localhost", "port": "1433", "database": "master"}
    )
    sink = MSSQLSink(target=target, stream_name="s", schema=schema, key_properties=[])

    table = arrow_bulkcopy.conform_arrow_table(
        pa.table({"col": arrow_values}), schema, sink.conform_name, sink.connector
    )
    record = sink.preprocess_record({"col": deserialize_json(record_value)}, {})

    assert table.column("col").to_pylist() == [record["col"]]
    assert deserialize_json(record["col"]) == deserialize_json(record_value)
