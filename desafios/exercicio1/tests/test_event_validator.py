import unittest
from event_validator import EventsValidator


class TestEventValidator(unittest.TestCase):
    required_fields = ["name", "age", "job"]
    schema_obj = {
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

    event_validator = EventsValidator(required_fields, schema_obj)

    def test_has_required(self):
        """
        Sends an valid event to _has_required, based on schema obj and required fields created on global scope of
        test class.
        """

        event = {
            "name": "Pedro",
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00
            }
        }
        self.assertTrue(self.event_validator._has_required(event))

    def test_hasnt_required(self):
        """
        Sends an invalid event to _has_required, that misses an required field, based on schema_obj and required_fields
        created on global scope of test class.
        """

        event = {
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00
            }
        }
        with self.assertRaises(ValueError) as context:
            self.event_validator._has_required(event)
        self.assertIsInstance(context.exception, ValueError)

    def test_fit_fields(self):
        """
        Sends an valid event to _fit_fields, that fits with all fields expected on schema_obj, created on global scope
         of test class.
        """
        event = {
            "name": "Pedro",
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00
            }
        }
        self.assertTrue(self.event_validator._fit_fields(event))
        
    def test_doesnt_fit_fields_extra_field(self):
        """
        Sends an invalid event to _fit_fields, that unfits the fields expected on schema_obj, created on global scope of
         test class, having one field that is not listed in this schema.
        """
        event = {
            "name": "Pedro",
            "age": 28,
            "dont_fit": True,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00
            }
        }
        with self.assertRaises(ValueError) as context:
            self.event_validator._fit_fields(event)
        self.assertIsInstance(context.exception, ValueError)

    def test_type_match_schema(self):
        """
        Sends an valid event to _type_match_schema, that the fields matchs with all types expected on schema_obj,
         created on global scope of test class.
        """
        event = {
            "name": "Pedro",
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00
            }
        }
        self.assertTrue(self.event_validator._type_match_schema(
            self.schema_obj["job"]["type"], "job", event["job"] 
        ))
        
    def test_type_mismatch_schema(self):
        """
        Sends an invalid event to _type_match_schema, that the field 'job' mismatchs with the type expected
        on schema_obj, created on global scope of test class.
        """
        event = {
            "name": "Pedro",
            "age": "28",
            "job": ["Engineer"]
        }
        with self.assertRaises(TypeError) as context:
            self.event_validator._type_match_schema(self.schema_obj["job"]["type"], "job", event["job"])
        self.assertIsInstance(context.exception, TypeError)

    def test_valid_object(self):
        """
        Sends an valid event to valid_object, that will test all other methods, checking with the provided schema_obj
         created on global scope of test class.
        """
        event = {
            "name": "Pedro",
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00,
                "available_offices": [
                    "Belo Horizonte",
                    "São Paulo"
                ]
            }
        }
        self.assertTrue(self.event_validator.valid_object(event))

    def test_invalid_object(self):
        """
        Sends an invalid event to valid_object, that have an mismatch of datatype in 'available_offices' field,
         based on provided schema_obj created on global scope of test class.
        """
        event = {
            "name": "Pedro",
            "age": 28,
            "job": {
                "title": "Engineer",
                "company": "Iti",
                "wage": 25000.00,
                "available_offices": [
                    "Belo Horizonte",
                    21
                ]
            }
        }
        with self.assertRaises(Exception) as context:
            self.event_validator.valid_object(event)
        self.assertIsInstance(context.exception, Exception)


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
