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
