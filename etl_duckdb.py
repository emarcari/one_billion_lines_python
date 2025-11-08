import duckdb
import time


def create_duckdb():
    """
    Create a DuckDB in-memory database, read the CSV file, and compute
    the min, mean, and max temperatures for each station.
    """
    duckdb.sql(
        """
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3,1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("data/measurements100M.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3,1)'})
        GROUP BY station
        ORDER BY station
    """
    ).show()


if __name__ == "__main__":
    import time

    print("Starting DuckDB processing.")
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
