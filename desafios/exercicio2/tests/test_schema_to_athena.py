import unittest
from json_schema_to_hive import SchemaToAthena


class TestSchemaToAthena(unittest.TestCase):
    title = ("Test Schema", "test_schema")
    schema_obj = {
        "type": "object",
        "title": f"{title[0]}",
        "description": "The root schema comprises the entire JSON document.",
        "required": [
            "name",
            "age",
            "job"
        ],
        "properties":{
            "name": {
                "type": "string"
            },
            "age": {
                "type": "integer"
            },
            "have_children": {
                "type": "boolean",
            },
            "job": {
                "type": "object",
                "required": [
                    "title",
                    "wage",
                    "company"
                ],
                "properties": {
                    "title": {
                        "type": "string",
                    },
                    "company": {
                        "type": "string",
                    },
                    "wage": {
                        "type": "double",
                    },
                    "available_offices": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }

    cols = "'name' string,\n'age' int,\n'have_children' boolean,\n" \
           "'job' struct<'title':string,'company':string,'wage':double,'available_offices':array<string>>"
    schema_to_athena = SchemaToAthena(schema_obj)
    expected_table_query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {title[1]}({cols}) " \
                           f"ROW FORMAT '{schema_to_athena.row_format}' LOCATION '{schema_to_athena.location}'"

    def test_get_table_name(self):
        """Tests function that generates the table name. For testing purposes, table name/title of schema were defined
        in the same place, in global scope of test class.
        """
        expected_table_name = self.title[1]
        self.assertEqual(expected_table_name, self.schema_to_athena._get_table_name())

    def test_get_basic_struct_properties(self):
        """Tests function that generates list of basic properties, meaning that it wont have arrays or objects in the
        properties, only basic types.
        The list expected is based on the properties defined, and on athena documentation to create cols for table.
        """
        properties = {
            "name": {
                "type": "string"
            },
            "age": {
                "type": "integer"
            },
            "have_children": {
                "type": "boolean",
            }
        }
        expected_list = ["'name' string", "'age' int", "'have_children' boolean"]
        self.assertEqual(expected_list, self.schema_to_athena._get_struct_properties(properties))

    def test_get_list_basic_array(self):
        """Tests function that finds and return the type of data for an array when the data tye isn't an object.
        The string expected as result is the type defined in items dict.
        """
        struct = {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        self.assertEqual(struct['items']['type'], self.schema_to_athena._get_list(struct['items']))

    def test_get_basic_array_struct_properties(self):
        """Tests function that generates list of properties, with an array type and basic types.
        The list expected is based on the properties defined, and on athena documentation
        to create cols for create table.
        """
        properties = {
            "title": {
                "type": "string",
            },
            "company": {
                "type": "string",
            },
            "wage": {
                "type": "double",
            },
            "available_offices": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        }
        expected_list = ["'title' string", "'company' string", "'wage' double", "'available_offices' array<string>"]
        self.assertEqual(expected_list, self.schema_to_athena._get_struct_properties(properties))

    def test_get_simple_sub_struct(self):
        """Tests function that finds and return the type of data for an struct/dict when the data tye isn't 'complex',
        like arrays and other structs; Meaning that, it will recieve and dict with only basic data
        The string expected as result is each key of properties dict, separeted by ':' of it type, and a ',' separeting
        each property.
        """
        properties = {
                "title": {
                    "type": "string",
                },
                "company": {
                    "type": "string",
                },
                "wage": {
                    "type": "double",
                }
            }
        expected_string = "'title':string,'company':string,'wage':double"
        self.assertEqual(expected_string, self.schema_to_athena._get_sub_struct(properties))

    def test_get_sub_struct_with_list(self):
        """Tests function that finds and return the type of data for an struct/dict when the data type is 'complex',
        like arrays and other structs.
        The string expected as result is each key of properties dict, separeted by ':' of it type, and a ',' separeting
        each property.
        """
        properties = {
            "title": {
                "type": "string",
            },
            "company": {
                "type": "string",
            },
            "wage": {
                "type": "double",
            },
            "available_offices": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        }
        expected_string = "'title':string,'company':string,'wage':double,'available_offices':array<string>"
        self.assertEqual(expected_string, self.schema_to_athena._get_sub_struct(properties))

    def test_get_complex_struct_properties(self):
        """Tests function that receives property schema and process this schema to an list of properties that will be
        saved on athenas. The expected result is based on the properties setted in dict, and in the query used
        to create table documented in athena
        """
        properties = {
            "name": {
                "type": "string"
            },
            "age": {
                "type": "integer"
            },
            "have_children": {
                "type": "boolean",
            },
            "job": {
                "type": "object",
                "required": [
                    "title",
                    "wage",
                    "company"
                ],
                "properties": {
                    "title": {
                        "type": "string",
                    },
                    "company": {
                        "type": "string",
                    },
                    "wage": {
                        "type": "double",
                    },
                    "available_offices": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            }
        }
        expected_list = ["'name' string", "'age' int", "'have_children' boolean",
                         "'job' struct<'title':string,'company':string,'wage':double,'available_offices':array<string>>"
                         ]
        self.assertEqual(expected_list, self.schema_to_athena._get_struct_properties(properties))

    def test_table_cols(self):
        """Tests function that receives property schema and process this schema to an string of properties that will be
        saved on athenas. The expected result is based on the properties setted in dict, and in the query used
        to create table documented in athena
        """
        properties = {
            "name": {
                "type": "string"
            },
            "age": {
                "type": "integer"
            },
            "have_children": {
                "type": "boolean",
            },
            "job": {
                "type": "object",
                "required": [
                    "title",
                    "wage",
                    "company"
                ],
                "properties": {
                    "title": {
                        "type": "string",
                    },
                    "company": {
                        "type": "string",
                    },
                    "wage": {
                        "type": "double",
                    },
                    "available_offices": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            }
        }
        
        expected_string = "'name' string,\n'age' int,\n'have_children' boolean,\n'job' struct" \
                          "<'title':string,'company':string,'wage':double,'available_offices':array<string>>"
        self.assertEqual(expected_string, self.schema_to_athena._table_cols(properties))

    def test_table_query(self):
        """Tests function generates the complete string that is used to create an table in athenas.
        The expected result is based on the properties of schema_obj dict, in the title setted on global var, and in the
        query used to create table documented in athena
       """
        self.assertEqual(self.expected_table_query, self.schema_to_athena.create_table_query())


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
