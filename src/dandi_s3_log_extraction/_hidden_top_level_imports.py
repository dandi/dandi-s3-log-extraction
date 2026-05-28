"""
Including these directly within the top-level `__init__.py` makes them visible to autocompletion.

But we only want the imports to trigger, not for them to actually be exposed.
"""

from .summarize import generate_dandiset_summaries

_hide = True
