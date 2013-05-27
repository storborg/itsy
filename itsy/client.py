import requests


def get(url, referer, headers=None):
    headers = headers or {}
    headers.setdefault('User-Agent',
                       'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) '
                       'AppleWebKit/536.26 (KHTML, like Gecko) '
                       'Version/6.0 Mobile/10A5355d Safari/8536.25')
    headers.setdefault('DNT', '1')
    if referer:
        headers['Referer'] = referer
    r = requests.get(url, headers=headers)
    return r.text
