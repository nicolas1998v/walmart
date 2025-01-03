"""
Test suite for Walmart Pipeline.
Contains unit tests and integration tests for data processing pipeline components.
"""

from pathlib import Path

# Define test directory root
TEST_DIR = Path(__file__).parent

# Define paths to test resources
TEST_RESOURCES_DIR = TEST_DIR / "resources"
TEST_DATA_DIR = TEST_RESOURCES_DIR / "data"

# Ensure test directories exist
TEST_RESOURCES_DIR.mkdir(exist_ok=True)
TEST_DATA_DIR.mkdir(exist_ok=True)
