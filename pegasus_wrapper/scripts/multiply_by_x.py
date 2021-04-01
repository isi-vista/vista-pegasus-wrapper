# This script is for testing purposes only
import logging
import time

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    x = params.integer("x")
    logging.info("Reading from input file: %s", str(input_file_path.absolute()))
    with input_file_path.open() as input_file:
        with output_file_path.open("w") as output_file:
            for num in input_file:
                output_file.write(f"{int(num)*x}\n")

    logging.info("Writing to output file: %s", str(output_file_path.absolute()))

    # Pause so that we can examine the job on the SAGA cluster
    time.sleep(30)


if __name__ == "__main__":
    parameters_only_entry_point(main)
