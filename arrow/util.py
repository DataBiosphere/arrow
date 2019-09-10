import traceback
from functools import wraps
from sanic import response


def catch_and_report_exceptions():
    def decorator(f):
        @wraps(f)
        async def decorated(request, *args, **kwargs):
            try:
                return await f(request, *args, **kwargs)
            except Exception as err:
                return response.text(
                    "Error processing PFB: {0}\n{1}".format(err, traceback.format_exc()), 500)
        return decorated
    return decorator
