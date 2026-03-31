import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Add src to path so tests can import project modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield session
    session.stop()
