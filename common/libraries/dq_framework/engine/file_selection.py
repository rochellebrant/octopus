from pathlib import Path
from fnmatch import fnmatch
import os


def parse_patterns(s: str | None) -> list[str]:
    '''
    Turns a comma-separated string into a clean list of wildcard patterns.

    E.g.

    "abc_*"         -> ["abc_*"]

    "abc_*, def"    -> ["abc_*", "def"]

    "" or None      -> []

    '''
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


# Given a config_rel_path and some include/exclude patterns, which file.extension configs should I run?
def discover_file_names(
    config_base_dir: str,
    config_rel_path: str,
    extension: str,
    include: str,
    exclude: str | None = None
    ) -> list[str]:
    
    # Validate Directory Existence
    tables_dir = Path(config_base_dir) / "configs" / config_rel_path
    if not tables_dir.exists():
        raise NotADirectoryError(
            f"The configuration directory does not exist: {tables_dir}. "
            f"Check if 'config_rel_path' ({config_rel_path}) matches your folder structure."
        )
    
    # Filter out defaults.yaml and only look for table yamls
    try:
        files_with_extn = [
            f for f in tables_dir.glob(f"*.{extension}") 
            if f.stem != "defaults"
        ]
    except Exception as e:
        raise RuntimeError(f"Failed to scan directory {tables_dir}: {e}")
    
    # Pattern Matching Logic
    include_pats = parse_patterns(include) or ["*"]
    exclude_pats = parse_patterns(exclude)

    names = []
    for file in files_with_extn:
        name = file.stem
        if any(fnmatch(name, pattern) for pattern in include_pats):
            if not (exclude_pats and any(fnmatch(name, pattern) for pattern in exclude_pats)):
                names.append(name)
    
    # Enforce that at least one file must be found
    if not names:
        raise ValueError(
            f"No configuration files found in {tables_dir} matching "
            f"include='{include}' and exclude='{exclude}'."
        )

    return sorted(names)