from pandas import DataFrame

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