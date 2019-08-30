import fastavro
import traceback
import urllib.request
from sanic import response, Sanic
from arrow.translate import translate

app = Sanic('arrow')


@app.get('/_ah/warmup')
async def warmup(request):
    return response.text('OK')


@app.get('/status')
async def status(request):
    return response.text('OK')


@app.route('/avroToRawls', methods={'POST'})
async def avro_to_rawls(request):
    try:
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

    except Exception as err:
        return response.text(
            "Error processing PFB: {0}\n{1}".format(err, traceback.format_exc()), 500)


if __name__ == '__main__':
    app.run(port=8000)
