import urllib.parse


def sanitize_url(url):
    parts = urllib.parse.urlparse(url)
    parts = parts._replace(
        path=urllib.parse.quote(parts.path),
        params=urllib.parse.quote(parts.params),
        query=urllib.parse.quote(parts.query),
        fragment=urllib.parse.quote(parts.fragment)
    )
    return urllib.parse.urlunparse(parts)
