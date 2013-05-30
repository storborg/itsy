import requests


class Client(object):
    """
    HTTP client interface owned by an Itsy worker. May be customized with user
    agent and proxy configuration.
    """
    default_user_agent = ('Mozilla/5.0 (compatible; Googlebot/2.1; '
                          '+http://www.google.com/bot.html)')

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
