"""Root conftest.py: adds the project root to sys.path for all test imports.

This replaces the sys.path.insert() boilerplate that was in each test file.
pytest discovers this file automatically when running from the project root.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
