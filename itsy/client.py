import requests


class Client(object):
    default_user_agent = ('Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) '
                          'AppleWebKit/536.26 (KHTML, like Gecko) '
                          'Version/6.0 Mobile/10A5355d Safari/8536.25')

    def __init__(self, user_agent=default_user_agent, dnt=True, proxies=None):
        self.user_agent = user_agent
        self.dnt = dnt
        self.proxies = proxies

    def get(self, url, referer, headers=None):
        headers = headers or {}
        headers.setdefault('User-Agent', self.user_agent)

        if self.dnt:
            headers.setdefault('DNT', '1')

        if referer:
            headers['Referer'] = referer
        r = requests.get(url, headers=headers, proxies=self.proxies)
        return r.text
