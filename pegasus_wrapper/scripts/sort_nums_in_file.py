# This script is for testing purposes only
import logging

from immutablecollections import immutableset
from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    logging.info("Reading from input file: %s", str(input_file_path.absolute()))
    with input_file_path.open() as input_file:
        nums = [int(x.strip()) for x in input_file if x.strip() != ""]

    nums.sort()

    output_file_path.write_text("\n".join(immutableset([str(x) for x in nums])))


if __name__ == "__main__":
    parameters_only_entry_point(main)
