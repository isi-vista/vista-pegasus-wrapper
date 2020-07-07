# This script is for testing purposes only
import logging
import time

from immutablecollections import immutableset
from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    logging.info("Reading from input file: %s", str(input_file_path.absolute()))
    with input_file_path.open() as input_file:
        nums = [int(x) for x in input_file]

    nums.sort()

    # Pause so that we can examine the job on the SAGA cluster
    time.sleep(30)

    logging.info("Writing to output file: %s", str(input_file_path.absolute()))
    output_file_path.write_text("\n".join(f"{n}" for n in immutableset(nums)))


if __name__ == "__main__":
    parameters_only_entry_point(main)
