from enum import Enum
from importlib import import_module
from typing import Callable, cast, Protocol


class Engine(Enum):
    """Workflow Manager engine flavors."""
    pyflow = 'pyflow'

    def __str__(self):
        return self.value


class GenerateProto(Protocol):
    """Need a protocol to define the type returned by importlib."""
    generate: Callable
    parse_args: Callable


def get_engine_generator(engine: Engine) -> GenerateProto:
    """Dynamically loads the engine generate function."""
    generator_function = cast(GenerateProto, import_module(f'autosubmit.generators.{engine.value}'))
    return generator_function


__all__ = [
    'Engine',
    'get_engine_generator'
]
