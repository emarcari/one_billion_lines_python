import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time

CONCURRENCY = cpu_count()

total_lines = 100_000_000
chunksize = 10_000_000
filename = "data/measurements100M.txt"


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Process a single chunk of the DataFrame to compute min, max, and mean temperatures per station.

    :param chunk: A pandas DataFrame chunk.
    :return: A DataFrame with min, max, and mean temperatures per station.
    """
    aggregated = (
        chunk.groupby("station")["measure"].agg(["min", "max", "mean"]).reset_index()
    )
    return aggregated


def create_df_with_pandas(filename, total_lines, chunksize=chunksize) -> pd.DataFrame:
    """
    Create a DataFrame by reading a large CSV file in chunks and processing each chunk in parallel.

    :param filename: Path to the CSV file.
    :param total_lines: Total number of lines in the CSV file.
    :param chunksize: Number of lines per chunk.
    :return: A pandas DataFrame with aggregated min, max, and mean temperatures per station.
    """
    total_chunks = total_lines // chunksize + (1 if total_lines % chunksize else 0)
    results = []

    with pd.read_csv(
        filename,
        sep=";",
        header=None,
        names=["station", "measure"],
        chunksize=chunksize,
    ) as reader:
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processing"):
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = (
        final_df.groupby("station")
        .agg({"min": "min", "max": "max", "mean": "mean"})
        .reset_index()
        .sort_values("station")
    )

    return final_aggregated_df


if __name__ == "__main__":
    print("Starting file processing.")
    start_time = time.time()
    df = create_df_with_pandas(filename, total_lines, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")
