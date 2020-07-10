"""
Removes checkpoints from an experiment directory.

Our wrapper writes its own checkpoints to the locator directory for each completed job.
If these checkpoints exist, `WorkflowBuilder` won't add the corresponding jobs
to the generated DAX.

If you want to force rerun of a job, apply this script to a directory.
All checkpoints from that directory and its sub-directories will be removed.
"""
import sys
from pathlib import Path

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Expected one argument, the root of the directory tree to clear checkpoints from"
        )
    root_dir = Path(sys.argv[1])  # pylint:disable=invalid-name
    print(f"Removing checkpoints under {root_dir}")
    for ckpt_file in root_dir.rglob("___ckpt"):
        print(f"Removing {ckpt_file}")
        ckpt_file.unlink()

# Should we move this file to scripts?
