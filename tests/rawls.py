def add_update_attribute(name, value):
    return {
        'op': 'AddUpdateAttribute',
        'attributeName': name,
        'addUpdateAttribute': value
    }
