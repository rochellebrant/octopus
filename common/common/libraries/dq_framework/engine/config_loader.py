import yaml
import os
from pathlib import Path


def load_yaml(path: str) -> dict:
    '''
    Loads a YAML file with error handling for OS and Syntax issues.
    '''
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Syntax error in YAML file at {path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Could not read YAML file at {path}: {e}")


def load_combined_config(base_path: str, config_group: str, table_yaml_name: str) -> dict:
    '''
    Loads and merges table configs with defaults, enforcing structure.
    '''
    configs_dir = Path(base_path) / "configs" / config_group
    defaults_path = configs_dir / "defaults.yaml"
    table_path = configs_dir / f"{table_yaml_name}.yaml"

    # Load defaults
    defaults_data = load_yaml(str(defaults_path))
    defaults = defaults_data.get("defaults", defaults_data)
    
    # Load Table Config
    try:
        table_cfg = load_yaml(str(table_path))
    except Exception as e:
        raise RuntimeError(f"Failed to load config for table '{table_yaml_name}': {e}")
    
    # Check if table config exists
    if not table_cfg:
        raise FileNotFoundError(
            f"Config '{table_yaml_name}.yaml' not found in path: {configs_dir}"
        )
    
    # Enforce mandatory fields (Validation)
    # If the YAML doesn't have a 'table_name', fail immediately
    if "table_name" not in table_cfg and table_yaml_name is None:
        raise KeyError(f"Table config '{table_path}' must contain a 'table_name' key.")
    
    # Merge logic
    merged_roles = {**(defaults.get("roles", {})), **(table_cfg.get("roles", {}))}
    
    default_checks = defaults.get("checks", [])
    table_checks = table_cfg.get("checks", [])

    # Ensure both are lists to prevent + operator errors
    if not isinstance(default_checks, list): default_checks = []
    if not isinstance(table_checks, list): table_checks = []

    # ensure both are lists even if YAML parsed them as None
    if default_checks is None: default_checks = []
    if table_checks is None: table_checks = []
    
    merged_checks = default_checks + table_checks

    return {
        "table": {
            "table_name": table_cfg.get("table_name", table_yaml_name),
            "roles": merged_roles,
            "config_group": config_group,
        },
        "merged_checks": merged_checks,
    }