import json
import falcon
import jsonschema
from functools import wraps


def validate(schema, altschema=None):
    """
    Validate against JSON schema an return something.

    Return a parsed object if there is a POST.
    If there is a get do not return anything just validate.
    """
    def decorator(func):
        """Decorator function."""
        @wraps(func)
        def wrapper(self, req, resp, *args, **kwargs):
            """Wrap it nicely."""
            if req.method == 'POST':
                try:
                    raw_json = req.stream.read()
                    obj = json.loads(raw_json.decode('utf-8'))
                except Exception:
                    raise falcon.HTTPBadRequest(
                        'Invalid data',
                        'Could not properly parse the provided data as JSON'
                    )
                if altschema:
                    schema_alt_eval(obj, altschema, schema, req)
                else:
                    schema_eval(obj, schema, req)

                return func(self, req, resp, *args, parsed=obj, **kwargs)
            elif req.method == 'GET' or req.method == 'DELETE' and isinstance(req.path.split('/')[-1], int):
                return func(self, req, resp, *args, **kwargs)
        return wrapper
    return decorator


def schema_eval(obj, schema, request=None):
    """Evaluate schema."""
    try:
        jsonschema.validate(obj, schema, format_checker=jsonschema.FormatChecker())
    except jsonschema.ValidationError as e:
        if request and request.method:
            raise falcon.HTTPBadRequest(
                'Failed data validation',
                e.message
            )
        else:
            raise jsonschema.ValidationError(
                'Failed data validation',
                e.message
            )


def schema_alt_eval(obj, altschema, schema, request=None):
    """Evaluate schema."""
    try:
        jsonschema.validate(obj, altschema, format_checker=jsonschema.FormatChecker())
    except jsonschema.ValidationError:
        schema_eval(obj, schema, request)
