from typing import Any, Tuple, Type, Union

def assert_type(object: Any, name: str, types: Union[Tuple[Type], Type]):
    
    if not isinstance(types, (tuple, list)):
        types = (types,)
    elif isinstance(types, list):
        types = tuple(types)
    
    typename = lambda x: x.__name__
    
    if not isinstance(object, types):
        raise TypeError(f"{name} must be of type \
                        {list(map(typename, types))} \
                        got type {typename(type(object))}")
