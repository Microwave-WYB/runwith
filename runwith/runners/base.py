"""
This module contains the base class for all runners of python functions.
"""

import sys
from typing import Any, Callable, Dict, Tuple

import cloudpickle
import sh

from runwith.common import Assets

PrepareFuncion = Callable[[Callable, Tuple, Dict[str, Any], str, str, bool], Assets]


class Runner:
    """Base runner for a python function."""

    def __init__(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict[str, Any],
        target_template: str,
        exec_template: str,
        prepare: PrepareFuncion,
        verbose: bool = True,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.target_template = target_template
        self.exec_template = exec_template
        self.prepare = prepare
        self.verbose = verbose

    def run(self) -> Any:
        """
        Run the job in a slurm environment.

        Returns:
            Any: return value of the function
        """
        assets = self.prepare(
            self.func,
            self.args,
            self.kwargs,
            self.target_template,
            self.exec_template,
            self.verbose,
        )
        sh.Command(assets.exec)(_out=sys.stdout, _err=sys.stderr)
        try:
            ret = cloudpickle.loads(assets.ret.read_bytes())
        except Exception as e:
            assets.cleanup()
            raise e
        assets.cleanup()
        return ret

    def __call__(self) -> Any:
        return self.run()
