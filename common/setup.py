from setuptools import setup, find_packages
from datetime import datetime

# Dynamic version: base version + date
base_version = "1.0.0"
date_str = datetime.now().strftime("%Y%m%d")
version = f"{base_version}+{date_str}"

setup(
    name="common_funcs",
    version=version,
    description="Common functions for XIO project",
    author="XIO Team",
    packages=find_packages(),
    install_requires=[],  
    python_requires=">=3.7",
) 