"""
This module contains decorators to run functions in different environments.
"""

from functools import wraps
from typing import Any, Callable, Dict, Union

from runwith.common import SH_TEMPLATE
from runwith.runners.interpreter import InterpreterRunner
from runwith.runners.slurm import SlurmOptions, SlurmRunner


def slurm(
    options: Union[SlurmOptions, Dict[str, Any]],
    sh_template: str = SH_TEMPLATE,
    verbose: bool = True,
) -> Callable:
    """
    Decorator to run a function with a slurm job.

    Example:
    slurm(
        SlurmOptions(
            job_name="my_job",
            time="1:00:00",
            cpus_per_task=1,
            mem="1000M",
            partition="short",
        ),
        template="module load singularity; singularity exec image.sif python {target}"
    )

    Args:
        options (Union[SlurmOptions, Dict[str, Any]]): slurm options.
        template (str): custom script template.
                The template must be contain a {target} placeholder.
        verbose (bool, optional): print the generated script. Defaults to True.

    Returns:
        Callable: decorated function
    """
    if isinstance(options, dict):
        options = SlurmOptions(**options)

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            job_runner = SlurmRunner(func, args, kwargs, sh_template, options, verbose)
            return job_runner()

        return wrapper

    return decorator


def interpreter(path: str, verbose: bool = True) -> Callable:
    """
    Decorator to run a function with a custom interpreter.

    Example:
    interpreter("/usr/bin/python3")

    Args:
        path (str): path to the custom interpreter
        verbose (bool, optional): print the generated script. Defaults to True.

    Returns:
        Callable: decorated function
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            interpreter_runner = InterpreterRunner(func, args, kwargs, path, verbose)
            return interpreter_runner()

        return wrapper

    return decorator
