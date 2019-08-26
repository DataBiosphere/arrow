import fastavro
import urllib.request
from sanic import response, Sanic
from arrow.translate import translate

app = Sanic('arrow')


@app.get('/status')
async def status(request):
    return response.text('OK')


@app.route('/avroToRawls', methods={'POST'})
async def avro_to_rawls(request):
    url = request.json['url']
    avro = urllib.request.urlopen(url)
    reader = fastavro.reader(avro)
    result = await translate(reader, {'b64-decode-enums': True, 'prefix-object-ids': True})
    return response.json(result)


if __name__ == '__main__':
    app.run(port=8000)
