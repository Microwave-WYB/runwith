"""
Common utilities for the runwith package.
"""

import random
import string
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

import cloudpickle
import sh

EXEC_TEMPLATE = """#!/bin/bash
python {target}

"""


TARGET_TEMPLATE = """import sys
import pickle
from pathlib import Path

with open("{pickle_path}", "rb") as f:
    func = pickle.load(f)
    ret = func()
    ret_path = Path("{ret_path}")
    with open(ret_path, "wb") as f:
        pickle.dump(ret, f)

"""


@dataclass
class Assets:
    """
    Class for assets managements
    """

    base: Path
    target: Path = field(init=False)
    dump: Path = field(init=False)
    ret: Path = field(init=False)
    exec: Path = field(init=False)
    log: Path = field(init=False)

    def __post_init__(self):
        self.base.parent.mkdir(parents=True, exist_ok=True)
        self.target = self.base.with_suffix(".target.py").absolute()
        self.dump = self.base.with_suffix(".dump.pickle").absolute()
        self.ret = self.base.with_suffix(".ret.pickle").absolute()
        self.exec = self.base.with_suffix(".sh").absolute()
        self.log = self.base.with_suffix(".log").absolute()

        for path in [self.target, self.dump, self.ret, self.exec, self.log]:
            if path.exists():
                path.touch()

    def cleanup(self):
        """
        Remove all the generated files.
        """
        for path in [self.target, self.dump, self.ret, self.exec, self.log]:
            if path.exists():
                path.unlink()
        if self.target.parent.exists():
            self.target.parent.rmdir()

    def __del__(self):
        if self.target.parent.exists():
            self.cleanup()


def prepare(
    func: Callable,
    args: Tuple,
    kwargs: Dict,
    target_template: str = TARGET_TEMPLATE,
    exec_template: str = EXEC_TEMPLATE,
    verbose: bool = True,
) -> Assets:
    """
    Prepare the function to be run with in a different environment.

    Args:
        func (Callable): function to be run.
        args (Tuple): arguments to the function.
        kwargs (Dict): keyword arguments to the function.
        target_template (str, optional): template for the target script. Defaults to TARGET_TEMPLATE.
        exec_template (str, optional): template for the shell script. Defaults to EXEC_TEMPLATE.
        verbose (bool, optional): print the generated script. Defaults to True.

    Returns:
        sh.Command: shell command to run the function.
    """

    assert "{pickle_path}" in target_template, (
        "template must contain {pickle_path} placeholder, but got " + target_template
    )
    assert "{ret_path}" in target_template, (
        "template must contain {ret_path} placeholder, but got " + target_template
    )
    assert "{target}" in exec_template, (
        "template must contain {target} placeholder, but got " + exec_template
    )

    def random_name() -> str:
        letters = string.ascii_lowercase
        name = "".join(random.choice(letters) for _ in range(10))
        return name

    assets = Assets(base=Path(f"runwith_{random_name()}", func.__name__).absolute())

    func = partial(func, *args, **kwargs)
    assets.dump.write_bytes(cloudpickle.dumps(func))

    if verbose:
        print(f"Pickled function to {assets.dump}")

    target_script = target_template.format(
        pickle_path=assets.dump,
        ret_path=assets.ret,
    )

    assets.target.write_text(target_script, encoding="utf-8")
    if verbose:
        print(f"Generated target:\n{target_script}")

    assets.exec.write_text(exec_template.format(target=assets.target), encoding="utf-8")

    sh.Command("chmod")("+x", assets.exec)
    if verbose:
        print(f"Generated executable:\n{exec_template.format(target=assets.target)}")

    return assets


def load_return(ret_path: Path) -> Any:
    """
    Load the return value from the return file.

    Args:
        ret_path (Path): path to the return file

    Returns:
        Any: return value
    """
    ret = cloudpickle.loads(ret_path.read_bytes())
    return ret
