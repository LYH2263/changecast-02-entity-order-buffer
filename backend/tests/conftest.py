import os
import tempfile

os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="changecast-test-")

import pytest

from app.db import DB_PATH
from app.seed import init_db


@pytest.fixture(autouse=True)
def fresh_db():
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    yield
    if DB_PATH.exists():
        DB_PATH.unlink()
