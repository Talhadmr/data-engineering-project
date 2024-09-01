from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

class Column:
    def __init__(self, df):
        self.df = df

    def grab_col_names(self, cat_th=10, car_th=20):
        cat_cols = [col for col in self.df.columns if self.df[col].dtypes == "O"]
        num_but_cat = [col for col in self.df.columns if self.df[col].nunique() < cat_th and
                       self.df[col].dtypes != "O"]
        cat_but_car = [col for col in self.df.columns if self.df[col].nunique() > car_th and
                       self.df[col].dtypes == "O"]
        cat_cols = cat_cols + num_but_cat
        cat_cols = [col for col in cat_cols if col not in cat_but_car]

        num_cols = [col for col in self.df.columns if self.df[col].dtypes != "O"]
        num_cols = [col for col in num_cols if col not in num_but_cat]

        return cat_cols, num_cols, cat_but_car

    def constant_columns(self, cols):
        for col in cols:
            if self.df[col].nunique() == 1:
                self.df.drop(col, axis=1, inplace=True)


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

@transformer
def execute_transformer_action_for_missing(df: DataFrame, *args, **kwargs):
    columns = Column(df)
    cat_cols, num_cols, cat_but_car = columns.grab_col_names()

    miss = MissingValues(df)

    miss_col = miss.missing_columns()
    print("Missing columns before drop:", miss_col)

    miss.drop_missing_columns(miss_col)
    miss_col = miss.missing_columns()
    print("Missing columns after drop:", miss_col)

    # Fill missing columns (using all columns with missing values)
    miss.fill_Missing_Columns(df, miss_col.keys())
    miss_col = miss.missing_columns()
    print("Missing columns after fill:", miss_col)
    
    return [df, cat_cols, num_cols, cat_but_car]
