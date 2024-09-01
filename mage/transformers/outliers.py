from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

class Outlier:
    def outlier_thresholds(self, df, col_name, q1=0.25, q3=0.75):
        quartile1 = df[col_name].quantile(q1)
        quartile3 = df[col_name].quantile(q3)
        interquantile_range = quartile3 - quartile1
        up_limit = quartile3 + 1.5 * interquantile_range
        low_limit = quartile1 - 1.5 * interquantile_range
        return low_limit, up_limit

    def check_outlier(self, df, col_name):
        low_limit, up_limit = self.outlier_thresholds(df, col_name)
        if df[(df[col_name] > up_limit) |
                     (df[col_name] < low_limit)].any(axis=None):
            return True
        else:
            return False

    def grab_outliers(self, df, col_name, index=False):
        low, up = self.outlier_thresholds(df, col_name)

        if (df[((df[col_name] < low) |
                       (df[col_name] > up))].shape[0] > 10):
            print(df[((df[col_name] < low) |
                             (df[col_name] > up))].head())
        else:
            print(df[((df[col_name] < low) |
                             (df[col_name] > up))])

        if index:
            outlier_index = df[((df[col_name] < low) |
                                       (df[col_name] > up))].index
            return outlier_index

    def outlier_columns(self, df, num_col):
        outlier_columns = []
        for col in num_col:
            if self.check_outlier(df, col):
                outlier_columns.append(col)
        return outlier_columns

    def replace_with_thresholds(self, df, cols):
        for col in cols:
            low_limit, up_limit = self.outlier_thresholds(df, col)
            df.loc[(df[col] < low_limit), col] = low_limit
            df.loc[(df[col] > up_limit), col] = up_limit
        return df
    
    def remove_outlier(self, df, cols):
        for col in cols:
            low_limit, up_limit = self.outlier_thresholds(df, col)
            df_without_outliers = df[~((df[col] < low_limit) | (df[col] > up_limit))]
        return df_without_outliers


@transformer
def execute_transformer_action_for_outlier(data, *args, **kwargs) -> DataFrame:
    df = data[0]
    cat_cols = data[1]
    num_cols = data[2]
    cat_but_car = data[3]
    
    df = pd.DataFrame(df)
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame")
    
    out = Outlier()
    out_col = out.outlier_columns(df, num_cols)
    print("Outlier columns before replace with thresholds: ", out_col)

    df = out.replace_with_thresholds(df, out_col)
    out_col = out.outlier_columns(df, num_cols)
    print("Outlier columns after replace with thresholds: ", out_col)
    
    return df