"""
This module contains the runner for a python function in a custom interpreter.
"""

from typing import Any, Callable, Dict, Tuple

from runwith.common import TARGET_TEMPLATE, prepare
from runwith.runners.base import Runner


class InterpreterRunner(Runner):
    """Runner for a python function in a custom interpreter."""

    def __init__(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict[str, Any],
        interpreter: str,
        verbose: bool = True,
    ) -> None:
        sh_template = f"#!/bin/bash\n{interpreter} {{target}}\n\n"
        super().__init__(
            func,
            args,
            kwargs,
            TARGET_TEMPLATE,
            sh_template,
            prepare,
            verbose,
        )
        self.interpreter = interpreter
