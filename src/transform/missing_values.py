from pandas import DataFrame

class MissingValues:
    def __init__(self, df):
        self.df = df

    def missing_columns(self):
        na_columns = [col for col in self.df.columns if self.df[col].isnull().sum() > 0]
        na_values = self.df[na_columns].isnull().sum()
        columns_na_values = dict(zip(na_columns, na_values))

        return columns_na_values

    def fill_Missing_Columns(self, df, columns, method="mean"):
        for col in columns:
            if df[col].dtype not in ['int64', 'float64']:  # Check if the column is numeric
                print(f"Skipping column {col} as it is not numeric.")
                continue

            if method == "mean":
                df[col].fillna(df[col].mean(), inplace=True)
            elif method == "median":
                df[col].fillna(df[col].median(), inplace=True)
            elif method == "mode":
                df[col].fillna(df[col].mode()[0], inplace=True)
            else:
                print("Please enter a valid method: mean, median, mode")
                break
        return df


    def drop_missing_columns(self, columns, threshold=0.9):
        for col in columns.keys():
            if self.df[col].isnull().sum() / self.df.shape[0] > threshold:
                self.df.drop(col, axis=1, inplace=True)