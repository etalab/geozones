import click
from contextlib import contextmanager


def _secho(template=None, **skwargs):
    def func(text, *args, **kwargs):
        text = text.format(*args, **kwargs)
        if template:
            text = template.format(text)
        click.secho(text, **skwargs)
    return func


title = _secho('>> {0}', fg='cyan', bold=True)
section = _secho('> {0}', bold=True)
info = _secho()
success = _secho(fg='green')
error = _secho(fg='red', bold=True)
warning = _secho(fg='yellow')


@contextmanager
def ok(text):
    try:
        click.secho('{0} ...... '.format(text), nl=False)
        yield
    except:
        error('ko')
        raise
    else:
        success('ok')


def unicodify(string):
    '''Ensure a string is unicode and serializable'''
    return string.decode('unicode_escape') if isinstance(string, bytes) else string
