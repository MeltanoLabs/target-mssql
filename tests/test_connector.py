"""Scoped metadata cache tests using real SQLite reflection and DDL."""

# ruff: noqa: S101
from contextvars import Context
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from singer_sdk.sql import SQLConnector

from target_mssql.connector import MSSQLConnector


@pytest.fixture
def connector(tmp_path):
    result = MSSQLConnector({'load_method': 'upsert'}, sqlalchemy_url=f"sqlite:///{tmp_path / 'cache.db'}")
    yield result
    result._engine.dispose()


def schema(count=17):
    return {'type': 'object', 'properties': {f'c{i}': {'type': 'integer'} for i in range(count)}}


def probe(connector, action):
    # Only replace the orchestration for probes that need reads at precise points.
    # Reflection and DDL still use the real SQLAlchemy engine.
    with patch.object(SQLConnector, 'prepare_table', lambda self, *args, **kwargs: action()):
        connector.prepare_table('sample', schema(1), [])


def test_unchanged_table_reads_metadata_once_per_prepare(connector, caplog):
    connector.create_empty_table('sample', schema(), [])
    original = SQLConnector.get_table_columns
    reads = []

    def read(self, *args, **kwargs):
        reads.append(args[0])
        return original(self, *args, **kwargs)

    with patch.object(SQLConnector, 'get_table_columns', read):
        with caplog.at_level('INFO'):
            connector.prepare_table('sample', schema(), [])
        assert reads == ['sample']
        assert sum('method=prepare_column elapsed=' in r.message for r in caplog.records) == 17
        assert sum('cache=miss' in r.message and 'elapsed=' in r.message for r in caplog.records) == 1
        connector.prepare_table('sample', schema(), [])
        assert reads == ['sample', 'sample']
        connector.get_table_columns('sample')
        connector.get_table_columns('sample')
        assert reads == ['sample'] * 4


def test_ddl_refreshes_cache_and_filtering_is_preserved(connector):
    connector.create_empty_table('sample', schema(1), [])

    def action():
        assert list(connector.get_table_columns('sample', ['C0'])) == ['c0']
        assert list(connector.get_table_columns('sample', [])) == ['c0']
        with connector._engine.begin() as conn:
            conn.execute(sa.DDL('ALTER TABLE sample ADD COLUMN added VARCHAR(30)'))
        columns = connector.get_table_columns('sample')
        assert set(columns) == {'c0', 'added'}
        assert columns['added'].type.length == 30
        # SQLite cannot ALTER COLUMN: drop/recreate exercises real DDL invalidation
        # and a changed reflected type. MSSQL ALTER is covered by integration tests.
        with connector._engine.begin() as conn:
            conn.execute(sa.DDL('DROP TABLE sample'))
            conn.execute(sa.DDL('CREATE TABLE sample (c0 VARCHAR(50))'))
        assert connector.get_table_columns('sample')['c0'].type.length == 50

    probe(connector, action)


@pytest.mark.parametrize('failure', ['read', 'ddl'])
def test_errors_propagate_and_scope_and_listener_are_removed(connector, failure):
    connector.create_empty_table('sample', schema(1), [])
    initial_listeners = len(connector._engine.dispatch.before_cursor_execute)
    error = RuntimeError('read failed')

    def action():
        connector.get_table_columns('sample')
        with connector._engine.begin() as conn:
            conn.execute(sa.DDL('ALTER TABLE nonexistent ADD COLUMN x INT'))

    if failure == 'read':
        with patch.object(SQLConnector, 'get_table_columns', side_effect=error):
            with pytest.raises(RuntimeError) as caught:
                probe(connector, lambda: connector.get_table_columns('sample'))
            assert caught.value is error
    else:
        with pytest.raises(sa.exc.OperationalError):
            probe(connector, action)
    assert len(connector._engine.dispatch.before_cursor_execute) == initial_listeners
    with connector._engine.begin() as conn:
        conn.execute(sa.DDL('ALTER TABLE sample ADD COLUMN added INT'))
    assert 'added' in connector.get_table_columns('sample')
    connector.prepare_table('sample', schema(1), [])


def test_cached_columns_and_types_are_independent(connector):
    original = {'Label': sa.Column('Label', sa.String(20, collation='example'), nullable=False)}

    def action():
        columns = connector.get_table_columns('sample', ['label'])
        columns['Label'].type.collation = None
        columns['Label'].nullable = True
        columns.clear()
        again = connector.get_table_columns('sample')
        assert again['Label'].type.collation == 'example'
        assert again['Label'].nullable is False
        assert connector.get_table_columns('sample', ['missing']) == {}

    with patch.object(SQLConnector, 'get_table_columns', return_value=original):
        probe(connector, action)


def test_nested_scopes_connectors_tables_and_contexts_are_isolated(connector, tmp_path):
    other = MSSQLConnector({'load_method': 'upsert'}, sqlalchemy_url=f"sqlite:///{tmp_path / 'other.db'}")
    connector.create_empty_table('sample', schema(1), [])
    connector.create_empty_table('other_table', schema(2), [])
    other.create_empty_table('sample', schema(3), [])

    def inner():
        with connector._engine.begin() as conn:
            conn.execute(sa.DDL('ALTER TABLE sample ADD COLUMN added INT'))
        assert 'added' in connector.get_table_columns('sample')

    def outer():
        assert len(connector.get_table_columns('sample')) == 1
        assert len(other.get_table_columns('sample')) == 3
        assert len(connector.get_table_columns('other_table')) == 2
        Context().run(probe, other, lambda: other.get_table_columns('sample'))
        probe(connector, inner)
        assert 'added' in connector.get_table_columns('sample')

    try:
        probe(connector, outer)
    finally:
        other._engine.dispose()


@pytest.mark.parametrize('load_method', ['upsert', 'overwrite'])
def test_new_table_and_overwrite_keep_sdk_behavior(tmp_path, load_method):
    connector = MSSQLConnector({'load_method': load_method}, sqlalchemy_url=f"sqlite:///{tmp_path / 'new.db'}")
    try:
        if load_method == 'overwrite':
            connector.create_empty_table('sample', schema(2), [])
        connector.prepare_table('sample', schema(1), [])
        assert set(connector.get_table_columns('sample')) == {'c0'}
    finally:
        connector._engine.dispose()

