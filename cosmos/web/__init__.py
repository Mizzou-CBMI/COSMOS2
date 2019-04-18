try:
    import flask
except ImportError:
    raise NotImplementedError("please install the [web] extra for web functionality")
