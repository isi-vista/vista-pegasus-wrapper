from pathlib import Path

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path: Path = params.existing_file("input_file")
    output_file_path: Path = params.creatable_file("output_file")
    x = params.integer("x")
    with open(str(input_file_path)) as input_file:
        with open(str(output_file_path)) as output_file:
            for num in input_file:
                output_file.write(f"{num*x}\n")




if __name__ == "__main__":
    parameters_only_entry_point(main)
