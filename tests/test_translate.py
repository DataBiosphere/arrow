import fastavro
import pytest

from arrow.translate import translate
from tests.avro import make_avro_file
from tests.pfb_schema import make_pfb_schema
from tests.rawls import add_update_attribute


@pytest.mark.asyncio
async def test_translate_none():
    assert await translate(None) is None


@pytest.mark.asyncio
async def test_translate_nothing():
    schema = make_pfb_schema()
    file = make_avro_file(schema)
    result = await translate(fastavro.reader(file))
    assert result == []


@pytest.mark.asyncio
async def test_ignore_metadata():
    schema = make_pfb_schema()
    file = make_avro_file(schema, [
        {'name': 'metadata', 'object': {}}
    ])

    result = await translate(fastavro.reader(file))
    assert result == []


@pytest.mark.asyncio
async def test_translate_record():
    schema = make_pfb_schema([person_entity_def])
    file = make_avro_file(schema, [
        {'name': 'person', 'id': '123', 'object': {
            'first_name': 'Test', 'last_name': 'Dummy', 'eye_color': 'gray'
        }}
    ])

    result = await translate(fastavro.reader(file))
    assert result == [
        {
            'name': '123',
            'entityType': 'person',
            'operations': [
                add_update_attribute('first_name', 'Test'),
                add_update_attribute('last_name', 'Dummy'),
                add_update_attribute('eye_color', 'gray')
            ]
        }
    ]


@pytest.mark.asyncio
async def test_filter_none_values():
    schema = make_pfb_schema([person_entity_def])
    file = make_avro_file(schema, [
        {'name': 'person', 'id': '123', 'object': {'first_name': 'Test'}}
    ])

    result = await translate(fastavro.reader(file))
    assert result == [
        {
            'name': '123',
            'entityType': 'person',
            'operations': [
                add_update_attribute('first_name', 'Test')
            ]
        }
    ]


@pytest.mark.asyncio
async def test_decode_enum_value():
    use_base64_encoded_enums = True

    schema = make_pfb_schema([make_sample_entity_def(use_base64_encoded_enums)])
    file = make_avro_file(schema, [
        {'name': 'sample', 'id': '123', 'object': {'tissue_type': tissue_type_b64_values['Tumor']}},
        {'name': 'sample', 'id': '456', 'object': {'tissue_type': tissue_type_b64_values['Normal']}},
        {'name': 'sample', 'id': '789', 'object': {'tissue_type': tissue_type_b64_values['Unknown']}}
    ])

    result = await translate(fastavro.reader(file), {'b64-decode-enums': use_base64_encoded_enums})
    assert result == [
        {
            'name': '123',
            'entityType': 'sample',
            'operations': [
                add_update_attribute('tissue_type', 'Tumor')
            ]
        },
        {
            'name': '456',
            'entityType': 'sample',
            'operations': [
                add_update_attribute('tissue_type', 'Normal')
            ]
        },
        {
            'name': '789',
            'entityType': 'sample',
            'operations': [
                add_update_attribute('tissue_type', 'Unknown')
            ]
        }
    ]


@pytest.mark.asyncio
async def test_disambiguate_enum_and_non_enum():
    """
    Verify enum value decoding when different entities have fields with the same names
    """
    schema = make_pfb_schema([make_sample_entity_def(), other_sample_entity_def])
    file = make_avro_file(schema, [
        {'name': 'sample', 'id': '123', 'object': {'tissue_type': tissue_type_b64_values['Tumor']}},
        {'name': 'other_sample', 'id': '456', 'object': {'tissue_type': 'not a tumor'}},
    ])

    result = await translate(fastavro.reader(file), {'b64-decode-enums': True})
    assert result == [
        {
            'name': '123',
            'entityType': 'sample',
            'operations': [
                add_update_attribute('tissue_type', 'Tumor')
            ]
        },
        {
            'name': '456',
            'entityType': 'other_sample',
            'operations': [
                add_update_attribute('tissue_type', 'not a tumor')
            ]
        }
    ]


@pytest.mark.asyncio
async def test_make_file_links():
    schema = make_pfb_schema([file_def])
    file = make_avro_file(schema, [
        {'name': 'file', 'id': '123', 'object': {'object_id': 'abc.de/12345'}}
    ])

    result = await translate(fastavro.reader(file))
    assert result == [
        {
            'name': '123',
            'entityType': 'file',
            'operations': [
                add_update_attribute('object_id', 'abc.de/12345')
            ]
        }
    ]
    file.seek(0)
    result = await translate(fastavro.reader(file), {'prefix-object-ids': True})
    assert result == [
        {
            'name': '123',
            'entityType': 'file',
            'operations': [
                add_update_attribute('object_id', 'drs://abc.de/12345')
            ]
        }
    ]


person_entity_def = {'name': 'person', 'type': 'record', 'fields': [
    {'name': 'first_name', 'default': None, 'type': ['null', 'string']},
    {'name': 'last_name', 'default': None, 'type': ['null', 'string']},
    {'name': 'eye_color', 'default': None, 'type': [
        'null',
        {'type': 'enum', 'name': 'person_eye_color', 'symbols':
            ['black', 'blue', 'brown', 'gray', 'green']}]}]}


# These are actual "base64-encoded" enum values from a Gen3 PFB.
#
# Avro enum values (symbols) must match [A-Za-z_][A-Za-z0-9_]* for which many values in the data
# dictionary do not conform. Therefore, Gen3 base64-encodes the values AND strips off any trailing
# '=' padding. Properly encoded base64 values always have a length that is a multiple of 4.
# Therefore, the padding can (and must) be restored before decoding. These particular 3 values are
# also a useful collection because they represent the 3 possible padding scenarios.
tissue_type_b64_values = {
    'Tumor': 'VHVtb3I',      # VHVtb3I=
    'Normal': 'Tm9ybWFs',    # Tm9ybWFs
    'Unknown': 'VW5rbm93bg'  # VW5rbm93bg==
}


def make_sample_entity_def(encode_enum_values=False):
    if encode_enum_values:
        symbols = list(tissue_type_b64_values.values())
    else:
        symbols = list(tissue_type_b64_values.keys())

    sample_entity_def = {'name': 'sample', 'type': 'record', 'fields': [
        # {'name': 'type', 'type': {'type': 'enum', 'name': 'sample_type', 'symbols': 'sample'}},
        {'name': 'tissue_type', 'default': None, 'type': [
            'null',
            {'type': 'enum', 'name': 'sample_tissue_type', 'symbols': symbols}
        ]}
    ]}
    return sample_entity_def


other_sample_entity_def = {'name': 'other_sample', 'type': 'record', 'fields': [
    {'name': 'tissue_type', 'default': None, 'type': ['null', 'string']}
]}

file_def = {'name': 'file', 'type': 'record', 'fields': [
    {'name': 'object_id', 'default': None, 'type': ['null', 'string']}
]}
