from src.convertor import Convertor
from src.db.db_connection import PostgreSQLDB
from src.db.db_connection  import PostgresDataHandler

import pandas as pd
import numpy as np
from sklearn.preprocessing import KBinsDiscretizer
from sklearn.tree import DecisionTreeClassifier, plot_tree
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)   

if __name__ == '__main__':
    data = pd.read_csv('data/flixpatrol.csv')

    postgres_handler = PostgresDataHandler()
    #postgres_handler.write(data, 'chiad')

    data_from_postgres = postgres_handler.read('chiad')
    
    df = pd.DataFrame(data_from_postgres)
    df['Watchtime'] = df['Watchtime'].str.replace(',', '').astype(int)

    binner = KBinsDiscretizer(n_bins=3, encode='ordinal', strategy='quantile')
    df['Watchtime_binned'] = binner.fit_transform(df[['Watchtime']]).astype(int)

    data_encoded = pd.get_dummies(df[['Type', 'Premiere', 'Genre']], drop_first=True)
 
    X = data_encoded
    y = df['Watchtime_binned']

    tree = DecisionTreeClassifier(criterion='entropy', max_depth=2, random_state=42)
    tree.fit(X, y)

    plt.figure(figsize=(12, 8))
    plot_tree(tree, feature_names=X.columns, class_names=['Low', 'Medium', 'High'], filled=True, rounded=True)
    plt.title("CHAID Decision Tree Approximation")
    plt.savefig('decision_tree.png')
  

    