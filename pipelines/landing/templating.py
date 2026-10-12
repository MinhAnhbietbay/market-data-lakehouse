from typing import Any

def render_template(template: str, values: dict[str, Any]) -> str:
    rendered = template

    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))

    return rendered

def render_mapping(mapping: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    rendered = {}

    for key, value in mapping.items():
        if isinstance(value, str):
            rendered[key] = render_template(template=value, values=values)
        else:
            rendered[key] = value

    return rendered