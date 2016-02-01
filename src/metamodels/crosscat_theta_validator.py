import json
import jsonschema
import pkgutil

class Validator(object):

    def __init__(self):
        schema_json = pkgutil.get_data(
            'bayeslite.metamodels', 'crosscat_theta.schema.json')
        self.schema = json.loads(schema_json)

    def validate(self, obj):
        """Validate a Crosscat theta object.

        The object should the json-deserialized version of something that would
        be stored in the theta_json column of the bayesdb_crosscat_theta
        column. Raises an exception when validation fails."""
        jsonschema.validate(obj, self.schema)
