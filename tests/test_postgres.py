import unittest

from midb.postgres import (
    PGConnectionParameters,
    PGSchemaDatamodel,
    PGSchemaParameters,
    PGTypes,
)


class TestPostgres(unittest.TestCase):
    def test_import(self):
        """Test that the postgres module can be imported."""
        self.assertIsNotNone(PGConnectionParameters)
        self.assertIsNotNone(PGSchemaParameters)
        self.assertIsNotNone(PGTypes)
        self.assertIsNotNone(PGSchemaDatamodel)

    def test_connection_parameters(self):
        """Test creating connection parameters."""
        params = PGConnectionParameters(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            dbname="test",
        )

        # Get the parameters as a dictionary
        params_dict = params.to_dict()

        # Check the values in the dictionary
        self.assertEqual(params_dict["host"], "localhost")
        self.assertEqual(params_dict["port"], 5432)
        self.assertEqual(params_dict["user"], "postgres")
        self.assertEqual(params_dict["password"], "password")
        self.assertEqual(params_dict["dbname"], "test")

        # Also test the URL generation
        expected_url = "postgresql://postgres:password@localhost:5432/test"
        self.assertEqual(params.to_url(), expected_url)


if __name__ == "__main__":
    unittest.main()
