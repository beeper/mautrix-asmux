from .upgrade_table import upgrade_table
from . import (v01_initial_revision, v02_room_delete_on_cascade, v03_add_user_api_token,
               v04_add_user_manager_url, v05_add_user_proxy_config, v06_add_appservice_push,
               v07_varchar_to_text, v08_remove_manager_url, v09_embedded_bridge_registration,
               v10_room_delete)

__all__ = ["upgrade_table"]
