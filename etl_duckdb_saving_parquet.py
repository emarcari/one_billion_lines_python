import duckdb
import time


def create_duckdb(output_parquet_file: str):
    """
    Create a DuckDB in-memory database, read the CSV file, and compute
    the min, mean, and max temperatures for each station.
    """
    result = duckdb.sql(
        """
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3,1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("data/measurements100M.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3,1)'})
        GROUP BY station
        ORDER BY station
    """
    )

    result.show()

    result.write_parquet(output_parquet_file)


if __name__ == "__main__":
    import time

    print("Starting DuckDB processing.")
    start_time = time.time()
    create_duckdb("data/measurements_summary.parquet")
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
