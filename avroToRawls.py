import fastavro
import itertools
import requests
import sys
import urllib.parse
import urllib.request
from itertools import islice

from arrow.translate import translate


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


# url = 'https://storage.googleapis.com/breilly-pfb-scratch/export_2019-09-04T01_15_19.avro'
ws_namespace = urllib.parse.quote(sys.argv[1])
ws_name = urllib.parse.quote(sys.argv[2])
url = sys.argv[3]
token = sys.argv[4]
env = sys.argv[5]
options = {'b64-decode-enums': True, 'prefix-object-ids': True}

avro = urllib.request.urlopen(url)
reader = fastavro.reader(avro)
result = translate(reader, options)
print(len(result))

headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {token}',
}

rawls_url = f'https://rawls.dsde-{env}.broadinstitute.org/api/workspaces/{ws_namespace}/{ws_name}/entities/batchUpsert'
print(rawls_url)
for data in split_seq(result, 1000):
    response = requests.post(rawls_url, headers=headers, json=data)
    print(response)
