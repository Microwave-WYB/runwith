"""
This modules contains utility functions related to slurm
"""

import json
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, NoReturn, Optional, TextIO, Tuple

import simple_slurm

from runwith.common import TARGET_TEMPLATE, load_return, prepare
from runwith.runners.base import Runner


@dataclass
class SlurmOptions:
    """Class to manage slurm options."""

    account: Optional[str] = None
    acctg_freq: Optional[str] = None
    array: Optional[str] = None
    batch: Optional[str] = None
    bb: Optional[str] = None
    bbf: Optional[str] = None
    begin: Optional[str] = None
    chdir: Optional[str] = None
    cluster_constraint: Optional[str] = None
    clusters: Optional[str] = None
    comment: Optional[str] = None
    constraint: Optional[str] = None
    container: Optional[str] = None
    container_id: Optional[str] = None
    contiguous: bool = False
    core_spec: Optional[int] = None
    cores_per_socket: Optional[int] = None
    cpu_freq: Optional[str] = None
    cpus_per_gpu: Optional[int] = None
    cpus_per_task: Optional[int] = None
    deadline: Optional[str] = None
    delay_boot: Optional[int] = None
    dependency: Optional[str] = None
    distribution: Optional[str] = None
    error: Optional[str] = None
    exclude: Optional[str] = None
    exclusive: Optional[str] = None
    export: Optional[str] = None
    export_file: Optional[str] = None
    extra: Optional[str] = None
    extra_node_info: Optional[str] = None
    get_user_env: Optional[str] = None
    gid: Optional[str] = None
    gpu_bind: Optional[str] = None
    gpu_freq: Optional[str] = None
    gpus: Optional[str] = None
    gpus_per_node: Optional[str] = None
    gpus_per_socket: Optional[str] = None
    gpus_per_task: Optional[str] = None
    gres: Optional[str] = None
    gres_flags: Optional[str] = None
    hold: bool = False
    ignore_pbs: bool = False
    input: Optional[str] = None
    job_name: Optional[str] = None
    kill_on_invalid_dep: Optional[str] = None
    licenses: Optional[str] = None
    mail_type: Optional[str] = None
    mail_user: Optional[str] = None
    mcs_label: Optional[str] = None
    mem: Optional[str] = None
    mem_bind: Optional[str] = None
    mem_per_cpu: Optional[str] = None
    mem_per_gpu: Optional[str] = None
    mincpus: Optional[int] = None
    network: Optional[str] = None
    nice: Optional[int] = None
    no_kill: bool = False
    no_requeue: bool = False
    nodefile: Optional[str] = None
    nodelist: Optional[str] = None
    nodes: Optional[str] = None
    ntasks: Optional[int] = None
    ntasks_per_core: Optional[int] = None
    ntasks_per_gpu: Optional[int] = None
    ntasks_per_node: Optional[int] = None
    ntasks_per_socket: Optional[int] = None
    open_mode: Optional[str] = None
    output: Optional[str] = None
    overcommit: bool = False
    partition: Optional[str] = None
    power: Optional[str] = None
    prefer: Optional[str] = None
    priority: Optional[str] = None
    profile: Optional[str] = None
    propagate: Optional[str] = None
    qos: Optional[str] = None
    quiet: bool = False
    reboot: bool = False
    requeue: bool = False
    reservation: Optional[str] = None
    signal: Optional[str] = None
    sockets_per_node: Optional[int] = None
    spread_job: bool = False
    switches: Optional[str] = None
    test_only: bool = False
    thread_spec: Optional[int] = None
    threads_per_core: Optional[int] = None
    time: Optional[str] = None
    time_min: Optional[str] = None
    tmp: Optional[str] = None
    tres_per_task: Optional[str] = None
    uid: Optional[str] = None
    use_min_nodes: bool = False
    verbose: bool = False
    wait: bool = False
    wait_all_nodes: Optional[int] = None
    wckey: Optional[str] = None
    wrap: Optional[str] = None

    @classmethod
    def loads(cls, data: Dict[str, Any]) -> "SlurmOptions":
        """
        Load options from a dictionary.

        Args:
            data (Dict[str, Any]): dictionary with options

        Returns:
            SlurmOptions: instance with the options
        """
        return cls(**data)

    @classmethod
    def load(cls, file: TextIO) -> "SlurmOptions":
        """
        Load options from a file.

        Args:
            file (TextIO): file with the options

        Returns:
            SlurmOptions: instance with the options
        """
        data = json.load(file)
        return cls.loads(data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the options to a dictionary.

        Returns:
            Dict[str, Any]: dictionary with the options
        """
        return {k: v for k, v in asdict(self).items() if v is not None}


class SlurmRunner(Runner):
    """
    Class to manage a job.

    Attributes:
        options (SlurmOptions): slurm options
        slurm (simple_slurm.Slurm): slurm object
    """

    def __init__(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict[str, Any],
        sh_template: str,
        options: SlurmOptions = SlurmOptions(),
        verbose: bool = True,
    ):
        super().__init__(func, args, kwargs, TARGET_TEMPLATE, sh_template, prepare, verbose)
        self.options = options
        self.slurm = simple_slurm.Slurm()
        self.slurm.add_arguments(**self.options.to_dict())

    def run(self) -> Any:
        """
        Run the job in a slurm environment.

        Returns:
            Any: return value of the function
        """
        assets = prepare(
            self.func, self.args, self.kwargs, self.target_template, self.exec_template
        )
        self.slurm.srun(str(assets.exec))
        ret = load_return(assets.ret)
        assets.cleanup()
        return ret

    def submit(self) -> int:
        """
        Submit the job to slurm.

        Returns:
            int: job id
        """
        try:
            assets = prepare(self.func, self.args, self.kwargs)
            sh_script = self.exec_template.format(target=assets.target)
            assets.exec.write_text(sh_script, encoding="utf-8")
            job_id = self.slurm.sbatch(str(assets.exec))
        except Exception as e:  # pylint: disable=broad-except
            print(e)
            job_id = -1
        return job_id

    def __str__(self) -> str:
        return (
            f"Slurm Job:\n"
            f"  Function: {self.func.__name__}\n"
            f"  Arguments: {self.args}\n"
            f"  Keyword Arguments: {self.kwargs}\n"
            f"  Options: {self.options}\n"
            f"  Template: {self.exec_template}\n"
        )


@dataclass
class JobGroup:
    """Class to manage multiple jobs."""

    jobs: List[SlurmRunner]

    def add_job(self, job: SlurmRunner):
        """
        Add a job to the manager.

        Args:
            job (Job): job to be added
        """
        self.jobs.append(job)

    def run_all(self) -> NoReturn:
        """
        Run all jobs in the manager in parallel.
        """
        raise NotImplementedError("This method is not implemented yet.")

    def submit_all(self) -> List[int]:
        """
        Submit all jobs to slurm.

        Returns:
            List[int]: job ids
        """
        job_ids = [job.submit() for job in self.jobs]
        for i, job_id in enumerate(job_ids):
            if job_id == -1:
                print(f"Failed to submit job:\n{self.jobs[i]}")
            else:
                print(f"Submitted job with job id {job_id}")
        return job_ids
