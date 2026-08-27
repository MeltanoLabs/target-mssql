"""Unit tests for target_mssql.arrow_bulkcopy (no live DB required)."""

# ruff: noqa: S101, S105

from __future__ import annotations

from target_mssql import arrow_bulkcopy


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
