import os
import sys
import random
import time


def check_args(file_args: list):
    """
    Sanity checks out input and prints out usage if input is not a positive integer.

    :param file_args: list of command line arguments.
    :return: None
    """
    try:
        if len(file_args) != 2 or int(file_args[1]) <= 0:
            raise Exception()
    except:
        print(
            "Usage:  create_measurements.sh <positive integer number of records to create>"
        )
        print("        You can use underscore notation for large number of records.")
        print("        For example:  1_000_000_000 for one billion")
        exit()


def build_weather_station_name_list(input_path: str) -> list:
    """
    Grabs the weather station names from example data provided in repo and dedups.

    :param input_path: path to read weather station names from.
    :return: list of weather station names.
    """
    station_names = []
    with open(input_path, "r", encoding="utf-8") as file:
        file_contents = file.read()

    for station in file_contents.splitlines():
        if "#" in station:
            next
        else:
            station_names.append(station.split(";")[0])

    return list(set(station_names))


def convert_bytes(num: float) -> str:
    """
    Convert bytes to a human-readable format (e.g., KiB, MiB, GiB).

    :param num: number of bytes.
    :return: formatted string representing the size in a human-readable format.
    """
    for x in ["bytes", "KiB", "MiB", "GiB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds: float) -> str:
    """
    Format elapsed time in a human-readable format.

    :param seconds: elapsed time in seconds.
    :return: formatted string representing the elapsed time.
    """
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(weather_station_names: list, num_rows_to_create: int) -> str:
    """
    Tries to estimate how large a file the test data will be.

    :param weather_station_names: list of weather station names.
    :param num_rows_to_create: number of rows to create.
    :return: estimated file size in a human-readable format.
    """
    max_string = float("-inf")
    min_string = float("inf")
    per_record_size = 0
    record_size_unit = "bytes"

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size = num_rows_to_create * per_record_size
    human_file_size = convert_bytes(total_file_size)

    return f"Estimated file size: {human_file_size}.\n(Note: This is just an estimate based on station name lengths and may vary.)"


def build_test_data(
    weather_station_names: list, num_rows_to_create: int, output_file_path: str
) -> None:
    """
    Generates and writes to file the requested size of test data.

    :param weather_station_names: list of weather station names.
    :param num_rows_to_create: number of rows to create.
    :param output_file_path: path to write the generated data to.

    :return: None
    """
    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9
    station_names_10k_max = random.choices(weather_station_names, k=10_000)
    batch_size = 10000  # instead of writing line by line to file, process a batch of stations and put it to disk
    chunks = num_rows_to_create // batch_size
    print("Building test data...")

    try:
        with open(output_file_path, "w") as file:
            progress = 0
            for chunk in range(chunks):

                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = "\n".join(
                    [
                        f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}"
                        for station in batch
                    ]
                )  # :.1f should quicker than round on a large scale, because round utilizes mathematical operation
                file.write(prepped_deviated_batch + "\n")

                # Update progress bar every 1%
                if (chunk + 1) * 100 // chunks != progress:
                    progress = (chunk + 1) * 100 // chunks
                    bars = "=" * (progress // 2)
                    sys.stdout.write(f"\r[{bars:<50}] {progress}%")
                    sys.stdout.flush()
        sys.stdout.write("\n")
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()

    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize(output_file_path)
    human_file_size = convert_bytes(file_size)

    print(f"Test data successfully written to {output_file_path}.")
    print(f"Actual file size:  {human_file_size}")
    print(f"Elapsed time: {format_elapsed_time(elapsed_time)}")


def main():
    """
    Main program execution.
    """

    check_args(sys.argv)
    num_rows_to_create = int(sys.argv[1])
    input_file_path = "./data/weather_stations.csv"
    output_file_path = "./data/measurements.txt"
    weather_station_names = []
    weather_station_names = build_weather_station_name_list(input_file_path)
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create, output_file_path)
    print("Test data build complete.")


if __name__ == "__main__":
    main()
exit()
