from datetime import timedelta

from lxml.cssselect import CSSSelector

from itsy import Itsy, Task, configure_logging


def explore_handler(task, doc):
    for url in doc.extract_links('.ranked-repositories h3 a'):
        if url.count('/') == 4:
            document_type = 'repo'
        elif url.count('/') == 3:
            document_type = 'user'
        else:
            continue
        yield Task(url=url, document_type=document_type)


def user_handler(task, doc):
    for url in doc.extract_links('.popular-repos .public a'):
        yield Task(url=url, document_type='repo')


def repo_handler(task, doc):
    yield Task(url=task.url + '/stargazers', document_type='stargazers')


def stargazers_handler(task, doc):
    for url in doc.extract_links('#watchers li > a'):
        yield Task(url=url, document_type='user')

    page_links = CSSSelector('.pagination a')(doc.lxml)
    if page_links:
        next_url = page_links[-1].attrib['href']
        yield Task(url=next_url, document_type='stargazers')


def main():
    itsy = Itsy()

    itsy.add_handler('repo', repo_handler)
    itsy.add_handler('user', user_handler)
    itsy.add_handler('explore', explore_handler)
    itsy.add_handler('stargazers', stargazers_handler)

    # Check these once at the beginning.
    itsy.add_seed('https://github.com/explore/month', 'explore')
    itsy.add_seed('https://github.com/explore/week', 'explore')

    # Check this one every 12 hours.
    itsy.add_seed('http://github.com/explore', 'explore',
                  interval=timedelta(hours=12))

    itsy.crawl()


if __name__ == '__main__':
    configure_logging()
    main()
