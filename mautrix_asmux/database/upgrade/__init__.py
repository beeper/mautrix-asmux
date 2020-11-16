from .upgrade_table import upgrade_table
from . import (initial_revision, room_delete_on_cascade, add_user_api_token, add_user_manager_url,
               add_user_proxy_config)

__all__ = ["upgrade_table"]
