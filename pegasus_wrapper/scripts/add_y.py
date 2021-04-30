# This script is for testing purposes only
# It accepts command line arguments rather than a parameters file
import argparse
import logging
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="Input file path")
    parser.add_argument("output_file", help="Output file path")
    parser.add_argument("--y", help="Y value to add to each entry in the input file path")

    args = parser.parse_args()
    logging.info("Reading from input file: %s", str(args.input_file.absolute()))
    with args.input_file.open() as input_f:
        with args.output_file.open("w") as output_f:
            for num in input_f:
                output_f.write(f"{int(num)+args.y}\n")

    logging.info("Writing to output file: %s", str(args.output_file.absolute()))

    # Pause so that we can examine the job on the SAGA cluster
    time.sleep(10)


if __name__ == "__main__":
    main()
