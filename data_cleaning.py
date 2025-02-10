import pandas as pd

class DataCleaning:
    """A utility class to clean data."""

    def remove_null_values(self, df):
        """Removes rows with null values."""
        return df.dropna()

    def remove_duplicates(self, df):
        """Removes duplicate rows."""
        return df.drop_duplicates()

    def standardise_columns(self, df):
        """Standardises column names to lowercase and replaces spaces with underscores."""
        df.columns = df.columns.str.lower().str.replace(' ', '_', regex=True)
        return df
