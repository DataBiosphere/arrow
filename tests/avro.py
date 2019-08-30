import io
import fastavro


def make_avro_file(schema, entities=None):
    if entities is None:
        entities = []
    file = io.BytesIO()
    fastavro.writer(file, schema, entities)
    file.seek(0)
    return file
