from pandas import DataFrame
import pandas as pd

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

        if (df[((df[col_name] < low) | (df[col_name] > up))].shape[0] > 10):
            print(df[((df[col_name] < low) | (df[col_name] > up))].head())
        else:
            print(df[((df[col_name] < low) | (df[col_name] > up))])
        if index:
            outlier_index = df[((df[col_name] < low) | (df[col_name] > up))].index
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
