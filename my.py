import pandas as pd
import matplotlib.pyplot as plt
import sqlite3 as sq
import os

def load_csv(file_path):
    """Loads a CSV file into a pandas DataFrame."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at: {file_path}")
    print(f"Loading CSV from: {file_path}")
    return pd.read_csv(file_path)


def validate_columns(df, required_columns):
    """Validates that required columns exist in the DataFrame."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    print("All required columns are present.")


def reshape_data(df, id_column, year_columns):
    """Reshapes a DataFrame from wide to long format."""
    print("Reshaping data from wide to long format...")
    melted_df = pd.melt(
        df,
        id_vars=[id_column],
        value_vars=year_columns,
        var_name='Year',
        value_name='Crime_Count'
    )
    # Handle missing or invalid data
    melted_df.dropna(subset=['Crime_Count'], inplace=True)
    melted_df['Crime_Count'] = pd.to_numeric(melted_df['Crime_Count'], errors='coerce')
    melted_df.dropna(subset=['Crime_Count'], inplace=True)
    print(f"Reshaped DataFrame:\n{melted_df.head()}")
    return melted_df


def save_to_sqlite(df, db_name, table_name):
    """Saves a pandas DataFrame to an SQLite database."""
    print(f"Saving data to SQLite database: {db_name}, table: {table_name}")
    conn = sq.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    return conn


def fetch_data_by_state(conn, table_name):
    """Fetches data grouped by state and year from the database."""
    query = f"""
        SELECT "State/UT" AS State, Year, SUM(Crime_Count) AS Crime_Count
        FROM {table_name}
        GROUP BY "State/UT", Year
        ORDER BY "State/UT", Year;
    """
    print("Executing SQL query to fetch data by state...")
    return pd.read_sql_query(query, conn)


def plot_data_by_state(data, state_column, year_column, value_column):
    """Plots data for each state."""
    print("Plotting data by state...")
    states = data[state_column].unique()  # Get unique states
    plt.figure(figsize=(12, 8))
    
    for state in states:
        state_data = data[data[state_column] == state]
        plt.plot(
            state_data[year_column],
            state_data[value_column],
            marker='o',
            linestyle='-',
            label=state
        )

    plt.title('Crime Trends by State Over the Years', fontsize=14, fontweight='bold')
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Number of Crimes', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1), title='States/UTs')
    plt.tight_layout()
    plt.show()


def main():
    # Configuration
    csv_path = r"C:\Users\khanm\Documents\PythonProjectCrimeRateAnalysis\en.csv"
    db_name = ":memory:"  # Use ":memory:" for an in-memory database
    table_name = "crimes_table"
    id_column = "State/UT"
    year_columns = ['2019', '2020', '2021']

    try:
        # Load and preprocess data
        df = load_csv(csv_path)
        validate_columns(df, [id_column] + year_columns)
        reshaped_df = reshape_data(df, id_column, year_columns)

        # Save to SQLite and query
        conn = save_to_sqlite(reshaped_df, db_name, table_name)
        data = fetch_data_by_state(conn, table_name)
        conn.close()

        if data.empty:
            print("No data available for plotting.")
            return

        # Plot the data by state
        plot_data_by_state(data, "State", "Year", "Crime_Count")
        print("Process completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
