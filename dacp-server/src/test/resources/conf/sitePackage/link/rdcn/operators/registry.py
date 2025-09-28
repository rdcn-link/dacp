# file: operators/__init__.py

import link.rdcn.operators.normalize_operator as normalize_operator

registry = {
    "normalize": normalize_operator.process
}

def get_operator(name):
    if name in registry:
        return registry[name]
    raise ValueError(f"Unknown operator: {name}")
