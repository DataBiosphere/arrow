def make_pfb_schema(data_dictionary=None):
    if data_dictionary is None:
        data_dictionary = []
    pfb_schema = {
        'name': 'Entity',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': ['null', 'string'], 'default': None},
            {'name': 'name', 'type': 'string'},
            {'name': 'object', 'type':
                [{'name': 'Metadata', 'type': 'record', 'fields': []}] + data_dictionary},
            {'name': 'relations', 'type': {
                'type': 'array', 'items': {
                    'name': 'Relation',
                    'type': 'record',
                    'fields': [
                        {'name': 'dst_id', 'type': 'string'},
                        {'name': 'dst_name', 'type': 'string'}
                    ]
                }
            }, 'default': []}
        ]
    }
    return pfb_schema
