import base64


async def translate(reader, options=None):
    if options is None:
        options = {}
    t = Translator(options)
    return t.translate(reader)


class Translator:

    def __init__(self, options=None):
        if options is None:
            options = {}
        defaults = {'b64-decode-enums': False, 'prefix-object-ids': False}
        self.options = {**defaults, **options}

    def translate(self, reader):
        if reader is None:
            return None

        enums = _list_enums(reader.writer_schema)
        results = [self._translate_record(record, enums)
                   for record in reader if record['name'] != 'metadata']
        return results

    def _translate_record(self, record, enums):
        entity_type = record['name']
        name = record['id']

        def make_op(key, value):
            if self.options['b64-decode-enums'] and (entity_type, key) in enums:
                value = _b64_decode(value).decode("utf-8")
            if self.options['prefix-object-ids'] and key == 'object_id':
                value = 'drs://' + value
            return _make_add_update_op(key, value)

        operations = [make_op(key, value)
                      for key, value in record['object'].items() if value is not None]
        return {
            'name': name,
            'entityType': entity_type,
            'operations': operations
        }


def _b64_decode(encoded_value):
    return base64.b64decode(encoded_value + "=" * (-len(encoded_value) % 4))


def _list_enums(schema):
    object_field = next(f for f in schema['fields'] if f['name'] == 'object')
    types = [t for t in object_field['type'] if t['name'] != 'Metadata']
    enums = [(entity_type['name'], field['name'])
             for entity_type in types
             for field in entity_type['fields']
             for enum in field['type'] if isinstance(enum, dict) and enum['type'] == 'enum']
    return enums


def _make_add_update_op(key, value):
    return {
        'op': 'AddUpdateAttribute',
        'attributeName': key,
        'addUpdateAttribute': value
    }
