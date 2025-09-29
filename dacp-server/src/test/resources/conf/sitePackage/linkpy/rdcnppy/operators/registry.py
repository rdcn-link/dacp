# file: operators/__init__.py

from link.rdcn.operators import normalize_operator

registry = {
    "normalize": normalize_operator.process
}

def get_operator(name):
    if name in registry:
        return registry[name]
    raise ValueError(f"Unknown operator: {name}")
