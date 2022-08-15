import json
import boto3

_SQS_CLIENT = None


def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")


def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''
    file = open("schema.json")
    schema = json.load(file)
    validator = EventsValidator(schema['required'], schema['properties'])
    try:
        if validator.valid_object(event):
            send_event_to_queue(event, 'valid-events-queue')
    except Exception as e:
        print(e)


class EventsValidator:
    def __init__(self, required: list, schema_obj: dict):
        self.schema_obj = schema_obj
        self.object_required = required
        # dict structure created to translate schema types to python types
        self.data_types = {
            "string": str,
            "object": dict,
            "integer": int,
            "boolean": bool,
            "array": list,
            "double": float,
            "float": float
        }

    def _has_required(self, event: dict) -> bool:
        """Checks if event has all required fields listed on object_required by doing an subset of object_required
        with event keys. If it doesnt have all required fields, an Value Error Exception is raised indicating which are
        the required fields, and the fields that came in the object.

            :param event: dict with current event that is being validated
            :return: boolean indicating if has all required fields
        """
        if not set(self.object_required).issubset(event.keys()):
            raise ValueError(f"Missing required fields. "
                             f"Required fields are: {self.object_required}, received fields are {event.keys()}")
        return True

    def _fit_fields(self, event: dict) -> bool:
        """Checks if event have fields that are not listed on schema by doing an subset of event keys
        with schema object keys. If it have not listed fields, an Value Error Exception is raised indicating which are
        the fields received, and the fields expected by the schema.

            :param event: dict with current event that is being validated
            :return: boolean indicating if event fields fit with schema fields
        """
        if not set(event.keys()).issubset(self.schema_obj.keys()):
            raise ValueError(f"Received field(s) that are not listed on schema."
                             f" Fields received: {event.keys()}, fields expected: {self.schema_obj.keys()}")
        return True

    def _type_match_schema(self, data_type: str, field_name: str, field_value) -> bool:
        """Checks if current value is the same that is expected in schema object. It uses an auxiliary dict that gets
        the string type from and translate this for python type. Than, field value can be validated with isinstance
        method. If an mismatch of type happens, an Type Error Exception is raised indicating what is the type expected
        for field, and what type was received.

            :param data_type: string with data type that will be validated
            :param field_name: string with the field name that will be validated
            :param field_value: value that came in event for current field that is being validated. Type of this param
                is part of validation, so it cannot be typed
            :return: boolean indicating if event fields type match with schema types
        """

        if not isinstance(field_value, self.data_types[data_type]):
            raise TypeError(f"Expected {data_type} for field '{field_name}'"
                            f" received {type(field_value)}")
        return True

    def valid_object(self, event: dict) -> bool:
        """Validate event object. If event is valid, return True, else, it will show what is wrong with current event
        and return False.
            :param event: dict with current event that is being validated
            :return: boolean indicating if event is valid
        """
        try:
            if self._has_required(event) and self._fit_fields(event):
                for key in event.keys():
                    if self._type_match_schema(self.schema_obj[key]['type'], key, event[key]):
                        if self.schema_obj[key]['type'] == 'object':
                            EventsValidator(self.schema_obj[key]['required'],
                                            self.schema_obj[key]['properties']).valid_object(event[key])
                        elif self.schema_obj[key]['type'] == 'array':
                            if self._type_match_schema(self.schema_obj[key]['items']['type'], key, event[key][0]):
                                if self.schema_obj[key]['items']['type'] == 'object':
                                    for sub_event in event[key]:
                                        EventsValidator(self.schema_obj[key]['items']['required'],
                                                    self.schema_obj[key]['items']['properties']).valid_object(sub_event)
                                else:
                                    for value in event[key]:
                                        self._type_match_schema(self.schema_obj[key]['items']['type'], key, value)
            return True
        except Exception as e:
            raise Exception(e)
