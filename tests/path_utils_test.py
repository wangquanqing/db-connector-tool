from db_connector.utils.path_utils import PathHelper
from pathlib import Path

config_dir = PathHelper.get_user_config_dir()
print(config_dir)
home_dir = PathHelper.get_user_home_dir()
print(home_dir)
success = PathHelper.ensure_dir_exists(Path("C:\\Users\\wangq\\AppData\\Roaming\\db_connector\\test_dir"))
print(success)
normalized = PathHelper.normalize_path("~/documents/../downloads")
print(normalized)
success = PathHelper.is_valid_path("C:/valid/path")
print(success)
abs_path = PathHelper.get_absolute_path("config/app.conf")
print(abs_path)
safe_path = PathHelper.safe_join("/safe/base", "subdir", "file.txt")
print(safe_path)
