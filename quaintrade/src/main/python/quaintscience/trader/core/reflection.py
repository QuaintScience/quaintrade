import importlib


@staticmethod
def get_fully_qualified_name(obj):
    """Get fully qualified name of an object"""

    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__  # Avoid reporting __builtin__
    return module + '.' + obj.__class__.__name__

@staticmethod
def dynamically_load_class(string):
    """Load a class when given a fully qualified name"""

    global_vars = globals()
    if string.find(".") >= 0:
        parts = ".".join(string.split(".")[:-1]), string.split(".")[-1]
    else:
        parts = global_vars["__name__"], string
    if len(parts) > 0:
        module_name, class_name = ".".join(parts[:-1]), parts[-1]
    else:
        module_name, class_name = global_vars["__name__"], None
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        submodule_name, cls_name = module_name.rsplit(".", 1)
        sub_module = importlib.import_module(submodule_name)
        Cls = getattr(sub_module, cls_name)  # pylint: disable=invalid-name
        return getattr(Cls, class_name)
    Cls = getattr(module, class_name)  # pylint: disable=invalid-name
    return Cls
