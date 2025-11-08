from csv import reader
from collections import defaultdict, Counter
from tqdm import tqdm
import time

NUMBER_OF_LINES = 10_000_000  # used for tqdm progress bar


def process_temperatures(path_to_csv: str) -> dict:
    """
    Process the temperature measurements from a CSV file and compute
    the min, mean, and max temperatures for each station.

    :param path_to_csv: Path to the CSV file containing temperature measurements.
    :return: A dictionary with station names as keys and a formatted string
             of min/mean/max temperatures as values.
    """
    minimals = defaultdict(lambda: float("inf"))
    maximums = defaultdict(lambda: float("-inf"))
    sums = defaultdict(float)
    measures = Counter()

    with open(path_to_csv, "r") as file:
        _reader = reader(file, delimiter=";")

        for row in tqdm(_reader, total=NUMBER_OF_LINES, desc="Processing"):
            station_name, temperature = str(row[0]), float(row[1])
            measures.update([station_name])
            minimals[station_name] = min(minimals[station_name], temperature)
            maximums[station_name] = max(maximums[station_name], temperature)
            sums[station_name] += temperature

    print("Data loaded. Computing statistics...")

    results = {}
    for station, qtd_measures in measures.items():
        mean_temp = sums[station] / qtd_measures
        results[station] = (minimals[station], mean_temp, maximums[station])

    print("Statistic computation done. Ordering...")
    sorted_results = dict(sorted(results.items()))

    formatted_results = {
        station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
        for station, (min_temp, mean_temp, max_temp) in sorted_results.items()
    }

    return formatted_results


if __name__ == "__main__":
    csv_path = "data/measurements10M.txt"

    print("Starting data processing.")
    start_time = time.time()
    results = process_temperatures(csv_path)
    end_time = time.time()

    for station, metrics in results.items():
        print(station, metrics, sep=": ")

    print(f"\nProcessing done. Time spent: {end_time - start_time:.2f} secs.")
