import json

from django import template

register = template.Library()


@register.filter
def pretty_json(value):
    try:
        return json.dumps(
            value,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
    except Exception:
        return str(value)
