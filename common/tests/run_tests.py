import os
import sys
import pytest
import gc

# 1. CRITICAL: Environment protections for Serverless Workspace Files
# This prevents the "OSError: [Errno 95] Operation not supported" for __pycache__
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTEST_ADDOPTS"] = "-p no:cacheprovider"

def main():
    print("Starting Pytest execution on Serverless compute...")
    
    # 2. Path Resolution
    cwd = os.getcwd()
    repo_root = os.path.abspath(os.path.join(cwd, "..")) if os.path.basename(cwd) == "tests" else cwd
        
    # Inject Repo Root into sys.path so local imports work
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    
    # Inject Sub-libraries (funcs, libraries)
    for sub in ["funcs", "libraries"]:
        sub_path = os.path.join(repo_root, sub)
        if sub_path not in sys.path:
            sys.path.insert(0, sub_path)

    tests_dir = os.path.join(repo_root, "tests")
    gc.collect() # Clean memory before starting

    # 3. Run Pytest
    # Note: We removed the '-o dont_write_bytecode' which was causing a warning
    exit_code = pytest.main([
        "-v", 
        tests_dir,
        f"--ignore={os.path.join(tests_dir, 'sample_models')}",
        "--import-mode=importlib",
        "-p", "no:cacheprovider",
        "-p", "no:autoreload" 
    ])
    
    if exit_code == 0:
        print("✅ All tests passed successfully!")
    else:
        print(f"❌ Pytest failed with exit code {exit_code}")

    sys.stdout.flush()
    if exit_code != 0:
        raise RuntimeError(f"Pytest failed with exit code {exit_code}")
    
    print("Execution finished gracefully.")

if __name__ == "__main__":
    main()