"""Test Configuration."""

import pytest


def pytest_configure(config: pytest.Config):
    config.addinivalue_line(
        "filterwarnings",
        r"once:Python 3\.\d+ (will reach|reached) its end of life:FutureWarning",
    )
