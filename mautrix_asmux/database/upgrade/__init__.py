from mautrix.util.async_db.upgrade import register_upgrade_table_parent_module

register_upgrade_table_parent_module("mautrix_asmux")

from . import initial_revision
