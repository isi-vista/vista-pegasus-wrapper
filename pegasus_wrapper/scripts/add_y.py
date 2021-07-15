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
    logging.info("Reading from input file: %s", str(args.input_file))
    with open(args.input_file) as input_f:
        with open(args.output_file, "w") as output_f:
            for num in input_f:
                output_f.write(f"{int(num)+int(args.y)}\n")

    logging.info("Writing to output file: %s", str(args.output_file))

    # Pause so that we can examine the job on the SAGA cluster
    time.sleep(10)


if __name__ == "__main__":
    main()
