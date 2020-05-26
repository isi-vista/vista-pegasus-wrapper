# This script is for testing purposes only
import time

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    x = params.integer("x")
    with input_file_path.open() as input_file:
        with open(str(output_file_path.absolute()), "w") as output_file:
            for num in input_file:
                output_file.write(f"{int(num)*x}\n")

    time.sleep(60)


if __name__ == "__main__":
    parameters_only_entry_point(main)
