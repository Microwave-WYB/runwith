# RunWith

Run Python functions with custom runners.

## Usage

Example: running a function using Slurm.

```python
from run_with.runners
from run_with.decorator import slurm

sh_template = """#!/bin/bash
module load singularity
singularity exec /path/to/container python {target}
"""

@slurm({"partition": "compute", "mem": "1000M", "job_name": "test"}, sh_template)
def run_with_slurm():
    print("Running with Slurm")
    return True

if __name__ == "__main__":
    print(run_with_slurm())
```

Example: running a function with a custom interpreter.

```python
import sys
from run_with.decorator import interpreter

@interpreter("/home/xxx/anaconda3/bin/python")
def run_with_custom():
    print("Running with custom interpreter")
    return sys.executable

def run_with_local():
    print("Running with local interpreter")
    return sys.executable

if __name__ == "__main__":
    print(run_with_custom())
    print(run_with_local())
```
