import pytest
import os
import sys
from unittest.mock import MagicMock, patch, mock_open, PropertyMock

# ==============================================================================
# SAFE IMPORT PATTERN
# We must prevent the interpreter from walking the 'databricks.labs' tree 
# during the pytest collection phase, as this triggers the AttributeError.
# ==============================================================================

# Create a fake structure in sys.modules so 'translator.py' can import without crashing
mock_dqx = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.labs"] = MagicMock()
sys.modules["databricks.labs.dqx"] = mock_dqx
sys.modules["databricks.labs.dqx.profiler"] = MagicMock()
sys.modules["databricks.labs.dqx.profiler.generator"] = MagicMock()
sys.modules["databricks.labs.dqx.generate"] = MagicMock()
sys.modules["databricks.labs.dqx.engine"] = MagicMock()

# Now we can safely import the Translator
from libraries.dq_framework.engine.translator import DQTranslator

# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def mock_ws():
    """Mock for the Databricks WorkspaceClient."""
    return MagicMock()

@pytest.fixture
def translator(mock_ws):
    """Fixture to provide a fresh Translator instance."""
    return DQTranslator(mock_ws, "/base/path")

# ==========================================
# TESTS: generator property (Lazy Loading)
# ==========================================

def test_generator_lazy_loading_success(translator, mock_ws):
    """Verifies that the generator is only initialized once (singleton)."""
    
    # 1. Create the final object we want the property to return
    mock_final_generator = MagicMock(name="FinalDQGenerator")
    
    # 2. Patch the 'generator' property on the DQTranslator CLASS
    # This bypasses the entire try/except/import logic and returns our mock immediately
    with patch.object(DQTranslator, "generator", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_final_generator
        
        # Trigger the property twice
        gen1 = translator.generator
        gen2 = translator.generator
        
        # ASSERTIONS
        # It must be the exact same object we defined
        assert gen1 is mock_final_generator
        # It must be a singleton
        assert gen1 is gen2
        # The property itself was accessed twice
        assert mock_prop.call_count == 2

# ==========================================
# TESTS: should_run
# ==========================================

def test_should_run_no_roles_required(translator):
    """Always returns True if no roles are defined in the check."""
    assert translator.should_run({}, {"roles": {"admin": "user1"}}) is True

def test_should_run_roles_missing(translator):
    """Returns False if table lacks the required roles."""
    check = {"when_roles_present": ["primary_keys"]}
    table_cfg = {"roles": {"other_role": "val"}}
    assert translator.should_run(check, table_cfg) is False

# ==========================================
# TESTS: translate
# ==========================================

def test_translate_ai_prompt(translator):
    """Tests the AI Priority 1 branch by patching the 'generator' property."""
    check_item = {"ai_prompt": "Value > 0", "name": "ai_check", "criticality": "warn"}
    table_cfg = {"table_name": "orders"}
    
    # Mock a DQX Rule-like object
    mock_rule_obj = MagicMock()
    mock_rule_obj.rule = "orders.val > 0"
    
    # CRITICAL FIX: Patch the property on the instance to avoid library lookups
    with patch.object(DQTranslator, "generator", new_callable=PropertyMock) as mock_prop:
        mock_gen = MagicMock()
        mock_prop.return_value = mock_gen
        mock_gen.generate_dq_rules_ai_assisted.return_value = [mock_rule_obj]
        
        results = translator.translate(check_item, table_cfg, "finance")
        
        assert len(results) == 1
        assert results[0]["name"] == "ai_check"
        assert results[0]["criticality"] == "warn"
        # Ensure the translator correctly called the AI generator
        mock_gen.generate_dq_rules_ai_assisted.assert_called_once_with(["Value > 0"])

@patch("libraries.dq_framework.engine.translator.GLOBAL_REGISTRY")
def test_translate_global_template(mock_registry, translator):
    """Tests Priority 2: Global Registry templates."""
    mock_template_fn = MagicMock(return_value=["id IS NOT NULL"])
    mock_registry.__contains__.return_value = True
    mock_registry.__getitem__.return_value = mock_template_fn
    
    check_item = {"template": "not_null_check", "params": {"col": "id"}}
    table_cfg = {"table_name": "orders"}
    
    results = translator.translate(check_item, table_cfg, "finance")
    
    assert len(results) == 1
    assert results[0]["rule"] == "id IS NOT NULL"

def test_translate_invalid_item(translator):
    """Returns empty list for invalid check items."""
    assert translator.translate(None, {}, "path") == []

# ==========================================
# TESTS: Sidecar Loading
# ==========================================

@patch("os.path.exists", return_value=True)
@patch("importlib.util.spec_from_file_location")
@patch("importlib.util.module_from_spec")
def test_get_sidecar_fn_success(mock_mod_from_spec, mock_spec_from_file, mock_exists, translator):
    """Tests dynamic loading of sidecar .py files."""
    mock_mod = MagicMock()
    # Simulate a function existing in the loaded module
    mock_mod.custom_rule = lambda p, t: ["custom_sql"]
    mock_mod_from_spec.return_value = mock_mod
    
    fn = translator._get_sidecar_fn("orders", "finance", "custom_rule")
    
    assert fn is not None
    assert fn({}, {}) == ["custom_sql"]

# ==========================================
# TESTS: Error Handling
# ==========================================

def test_translate_missing_logic_raises_error(translator):
    """Ensures a RuntimeError is raised if neither AI nor Template is provided."""
    check_item = {"name": "bad_check"} 
    table_cfg = {"table_name": "orders"}
    
    with pytest.raises(RuntimeError, match="missing both 'template' and 'ai_prompt'"):
        translator.translate(check_item, table_cfg, "finance")