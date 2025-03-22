"""
Tests for error handling in the midb.postgres module.
"""

import unittest

from midb.postgres import PGConnectionParameters, PGSchemaParameters, PGTypes


class TestSchemaParametersEdgeCases(unittest.TestCase):
    """Test edge cases for PGSchemaParameters."""

    def setUp(self):
        """Set up test fixtures."""
        self.types = PGTypes()
        self.dtype_map = {
            "id": self.types.BigInt,
            "name": self.types.VarChar,
            "timestamp": self.types.TimeStampTz,
            "value": self.types.DoublePrecision,
        }

    def test_schema_parameters_validation(self):
        """Test validation of PGSchemaParameters."""
        # Test with valid parameters
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        self.assertEqual(params.schema_name, "public")
        self.assertEqual(params.table_name, "test_table")
        self.assertEqual(params.dtype_map, self.dtype_map)
        self.assertEqual(params.time_index, "timestamp")
        self.assertEqual(params.primary_keys, ["id"])

    def test_schema_parameters_string_representation(self):
        """Test string representation of PGSchemaParameters."""
        params = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        str_repr = str(params)
        self.assertIn("public", str_repr)
        self.assertIn("test_table", str_repr)
        self.assertIn("timestamp", str_repr)

    def test_schema_parameters_equality(self):
        """Test equality comparison of PGSchemaParameters."""
        params1 = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        params2 = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        self.assertEqual(params1, params2)

    def test_schema_parameters_inequality(self):
        """Test inequality comparison of PGSchemaParameters."""
        params1 = PGSchemaParameters(
            schema_name="public",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        params2 = PGSchemaParameters(
            schema_name="different",
            table_name="test_table",
            dtype_map=self.dtype_map,
            time_index="timestamp",
            primary_keys=["id"],
        )
        self.assertNotEqual(params1, params2)


class TestConnectionParameters(unittest.TestCase):
    """Test the PGConnectionParameters class."""

    def test_connection_parameters_validation(self):
        """Test validation of PGConnectionParameters."""
        # Test with valid parameters
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )
        params_dict = params.to_dict()
        self.assertEqual(params_dict["host"], "localhost")
        self.assertEqual(params_dict["port"], 5432)
        self.assertEqual(params_dict["user"], "postgres")
        self.assertEqual(params_dict["password"], "password")
        self.assertEqual(params_dict["dbname"], "test")

    def test_connection_parameters_string_representation(self):
        """Test string representation of PGConnectionParameters."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            dbname="test",
        )
        str_repr = str(params)
        self.assertIn("localhost", str_repr)
        self.assertIn("5432", str_repr)
        self.assertIn("postgres", str_repr)
        self.assertIn("test", str_repr)
        self.assertNotIn("secret", str_repr)  # Password should not be exposed

    def test_connection_parameters_url(self):
        """Test URL generation of PGConnectionParameters."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )
        url = params.to_url()
        self.assertEqual(
            url, "postgresql://postgres:password@localhost:5432/test"
        )

    def test_connection_parameters_dict(self):
        """Test dictionary representation of PGConnectionParameters."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )
        params_dict = params.to_dict()
        self.assertEqual(params_dict["host"], "localhost")
        self.assertEqual(params_dict["port"], 5432)
        self.assertEqual(params_dict["user"], "postgres")
        self.assertEqual(params_dict["password"], "password")
        self.assertEqual(params_dict["dbname"], "test")
