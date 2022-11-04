from typing import Any
import logging
from jinja2 import Template

from dbtafutil.utils.constants import JINJA_TEMPLATES_DIRECTORY
from dbtafutil.utils.logger import Logger

logger = logging.getLogger(Logger.getRootLoggerName())

def read_jinja_template_file(template_file_name: str) -> Template:
    with open(JINJA_TEMPLATES_DIRECTORY / template_file_name) as f:
        template = f.read()
    return Template(template)

DBT_DAG_TEMPLATE = read_jinja_template_file("dbt_dag_template.jinja2")


def render_jinja_template(jinja_template: Template, **kwargs: Any) -> str:
    """Render a jinja template, raising an error if the rendering fails."""
    rendered_template = jinja_template.render(**kwargs)  # type: ignore
    if rendered_template is None:
        raise ValueError("Jinja template could not be rendered.")
    return rendered_template
