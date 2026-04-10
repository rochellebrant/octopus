import importlib.util
import os
import threading
import sys
from libraries.dq_framework.templates.common_templates import GLOBAL_REGISTRY

class DQTranslator:
    def __init__(self, ws_client, base_path):
        self.ws = ws_client
        self.base_path = base_path
        self._generator = None
        self._ai_lock = threading.Lock() # ensures AI generation is sequential so doesn't error

    @property
    def generator(self):
        """Lazy loader for the AI DQ Generator with fallback paths because it is changing as we develop lol."""
        if not self._generator:
            try:
                from databricks.labs.dqx.profiler.generator import DQGenerator
            except ImportError:
                try:
                    from databricks.labs.dqx.generate import DQGenerator
                except ImportError:
                    try:
                        from databricks.labs.dqx.engine import DQGenerator
                    except ImportError as e:
                        raise ImportError(
                            "Could not find DQGenerator in databricks.labs.dqx. "
                            "Ensure the library is installed on the cluster."
                        ) from e
            try:
                self._generator = DQGenerator(self.ws)
            except Exception as e:
                raise RuntimeError(f"Failed to initialize DQGenerator: {e}")
        return self._generator


    def should_run(self, check_item, table_cfg):
        required_roles = check_item.get("when_roles_present", [])
        if not required_roles:
            return True
        present_roles = table_cfg.get("roles", {}).keys()
        return all(role in present_roles for role in required_roles)


    def translate(self, check_item, table_cfg, config_rel_path):
        table = table_cfg.get('table_name', 'unknown_table')
        
        if check_item is None or not isinstance(check_item, dict):
            print(f"⚠️ SKIPPING: Invalid check item (None or non-dict) in config for {table}.")
            return []

        if not self.should_run(check_item, table_cfg):
            return []

        ai_prompt = check_item.get("ai_prompt")
        template  = check_item.get("template")
        criticality = check_item.get("criticality", "error").lower()
        rule_name   = check_item.get("name") or template or "ai_generated_check"
        
        rules_found = []

        try:
            # Priority 1: AI Prompt
            if ai_prompt:
                with self._ai_lock:
                    ai_output = self.generator.generate_dq_rules_ai_assisted([ai_prompt])
                    for r in ai_output:
                        # force YAML name onto the DQX Rule object
                        if hasattr(r, "rule"):
                            r.name = rule_name
                            # Append the WHOLE object 'r', not 'r.rule'
                            rules_found.append(r)
                        else:
                            rules_found.append(r)

            # Priority 2/3: Template logic
            elif template:
                sidecar_fn = self._get_sidecar_fn(table, config_rel_path, template)
                if sidecar_fn:
                    try:
                        rules_found = sidecar_fn(check_item.get("params", {}), table_cfg)
                    except Exception as e:
                        raise RuntimeError(f"Sidecar function '{template}' failed for table {table}: {e}")
                elif template in GLOBAL_REGISTRY:
                    try:
                        rules_found = GLOBAL_REGISTRY[template](check_item.get("params", {}), table_cfg)
                    except Exception as e:
                        raise RuntimeError(f"Global Registry template '{template}' failed for table {table}: {e}")
                else:
                    raise ValueError(f"Template '{template}' not found in Registry or Sidecar for table {table}.")
            
            else:
                raise ValueError(f"Check item in {table} missing both 'template' and 'ai_prompt'.")

        except Exception as e:
            raise RuntimeError(f"Translation Error for rule '{rule_name}' on table '{table}': {e}")

        if not isinstance(rules_found, list):
            rules_found = [rules_found]
        
        # OVERRIDE: Force the correct YAML criticality onto the DQX rule payload
        for r in rules_found:
            if isinstance(r, dict):
                r["criticality"] = criticality
            
        return [{
            "name": f"{rule_name}_{i}" if len(rules_found) > 1 else rule_name,
            "rule": r,
            "criticality": criticality
        } for i, r in enumerate(rules_found)]

        

    def _get_sidecar_fn(self, table_name, config_rel_path, fn_name):
        '''Internal helper to load custom python logic.'''
        py_path = os.path.join(self.base_path, "configs", config_rel_path, f"{table_name}.py")
        if os.path.exists(py_path):
            try:
                # Use table name as module name to avoid caching conflicts
                module_name = f"sidecar_{table_name}"
                spec = importlib.util.spec_from_file_location(module_name, py_path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
            except Exception as e:
                raise SyntaxError(f"Critical error loading sidecar file '{py_path}': {e}")
            return getattr(mod, fn_name, None)
        return None