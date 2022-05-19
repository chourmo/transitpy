"""
Unit and regression test for the transitpy package.
"""

# Import package, test suite, and other packages as needed
import sys

import pytest

import transitpy


def test_transitpy_imported():
    """Sample test, will always pass so long as import statement worked."""
    assert "transitpy" in sys.modules
