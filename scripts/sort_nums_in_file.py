from immutablecollections import immutableset
from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point


def main(params: Parameters):
    input_file_path = params.existing_file("input_file")
    output_file_path = params.creatable_file("output_file")
    with input_file_path.open() as input_file:
        nums = [int(x) for x in input_file]

    nums.sort()

    output_file_path.write_text("\n".join(immutableset(nums)))


if __name__ == "__main__":
    parameters_only_entry_point(main)
