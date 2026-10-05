"""Which version of EvolvingWorkflow this worker process runs.

Read ONCE at process start from the environment and imported into the workflow
sandbox via `imports_passed_through`, so every replay inside one worker process
sees the same value. Changing it means restarting the worker: this is how the lab
simulates "deploying new workflow code while old executions are still running".
"""
import os

VARIANT = os.environ.get("EVOLVING_VARIANT", "v1")
