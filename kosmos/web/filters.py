from . import bprint

@bprint.add_app_template_filter
def to_thumb(b):
    return 'yes' if b else 'no'