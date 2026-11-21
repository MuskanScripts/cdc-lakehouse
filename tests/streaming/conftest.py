import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root -> `streaming` package

from streaming.common.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("unit-tests", master="local[2]", s3=False, with_packages=True)
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
