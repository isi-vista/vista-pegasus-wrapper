# This script is for testing purposes only
from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    x = params.integer("x")
    print("This is a checkpoint !! ****")
    with input_file_path.open() as input_file:
        with output_file_path.open("w") as output_file:
            for num in input_file:
                output_file.write(f"{num*x}\n")


if __name__ == "__main__":
    parameters_only_entry_point(main)
