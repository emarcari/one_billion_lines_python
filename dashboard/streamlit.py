import duckdb
import pandas as pd
import streamlit as st


@st.cache_data
def load_data():
    """Load data from the Parquet file into a pandas DataFrame.

    :return: A pandas DataFrame containing the temperature summary data.
    """
    con = duckdb.connect()
    data = con.execute("SELECT * FROM 'data/measurements_summary.parquet'").df()
    con.close()

    return data


def main():
    st.title("Weather Station Temperature Summary")

    data_load_state = st.text('Loading data...')
    data = load_data()
    data_load_state.text("Done! (using st.cache_data)")

    st.write(
        "This dashboard displays the min, mean, and max temperatures for each station."
    )

    if st.checkbox('Show raw data'):
        st.subheader('Raw data')
        st.write(data)

    st.write("Summary Statistics:")
    st.write(data.describe())


if __name__ == "__main__":
    main()
