import fastavro
import urllib.request
from sanic import response, Sanic
from arrow.translate import translate
from arrow.util import catch_and_report_exceptions

app = Sanic('arrow')


@app.get('/_ah/warmup')
@catch_and_report_exceptions()
async def warmup(request):
    return response.text('OK')


@app.get('/status')
@catch_and_report_exceptions()
async def status(request):
    return response.text('OK')


@app.route('/avroToRawls', methods={'POST'})
@catch_and_report_exceptions()
async def avro_to_rawls(request):
    try:
        url = request.json['url']
    except KeyError:
        return response.text("Missing required 'url' property", 400)

    defaults = {'b64-decode-enums': True, 'prefix-object-ids': True}
    request_options = request.json.get('options', {})
    options = {**defaults, **request_options}

    avro = urllib.request.urlopen(url)
    reader = fastavro.reader(avro)
    result = await translate(reader, options)
    return response.json(result)


if __name__ == '__main__':
    app.run(port=8000)
