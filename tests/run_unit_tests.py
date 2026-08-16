import pytest
import sys
import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Navigate up from /tests/run_tests to the project root directory
project_root = os.path.dirname(os.path.dirname(notebook_path))


# Change current working directory to the project root in Workspace
os.chdir(f"/Workspace{project_root}")
print(f"Current working directory: {os.getcwd()}")

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["tests", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
print(f"Pytest returned code: {retcode}")


