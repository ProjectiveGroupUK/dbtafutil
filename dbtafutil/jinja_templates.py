from typing import Any

from jinja2 import Template
from pathlib import Path

PACKAGE_DIRECTORY = Path(__file__).parent.resolve()
JINJA_TEMPLATES_DIRECTORY = PACKAGE_DIRECTORY / "jinja_templates"


def read_jinja_template_file(template_file_name: str) -> Template:
    with open(JINJA_TEMPLATES_DIRECTORY / template_file_name) as f:
        template = f.read()
    return Template(template)


DBT_SET_TEMPLATE = read_jinja_template_file("dbt_set.py.jinja2")
#WRAPPER_DAG_TEMPLATE = read_jinja_template_file("wrapper_dag.py.jinja2")


class JinjaTemplateRenderError(Exception):
    pass


def render_jinja_template(jinja_template: Template, **kwargs: Any) -> str:
    """Render a jinja template, raising an error if the rendering fails."""
    rendered_template = jinja_template.render(**kwargs)  # type: ignore
    if rendered_template is None:
        raise JinjaTemplateRenderError("Jinja template could not be rendered.")
    return rendered_template
