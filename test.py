import matplotlib
matplotlib.use('TkAgg')  # Arka ucunu değiştir
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from CHAID import Tree

df = pd.read_csv('data/flixpatrol.csv')

df = df.drop(columns=['Rank', 'Title', 'Watchtime in Million'])
independent_variable_columns = ['Type', 'Premiere', 'Genre']
dep_variable = 'Watchtime'

df['Type'] = df['Type'].astype('category')  # Kategorik
df['Genre'] = df['Genre'].astype('category')  # Kategorik
df['Premiere'] = pd.to_numeric(df['Premiere'], errors='coerce')  # Geçersiz değerleri NaN yap
df['Premiere'] = df['Premiere'].fillna(df['Premiere'].mean())  # NaN değerleri ortalama ile doldur
df['Watchtime'] = df['Watchtime'].str.replace(',', '').astype(int)  # Sayısal

independent_vars_types = {
    'Type': 'nominal',
    'Premiere': 'ordinal',
    'Genre': 'nominal'
}

tree = Tree.from_pandas_df(
    df,
    independent_vars_types,
    dep_variable,
    max_depth=2,
    min_parent_node_size=100,
    min_child_node_size=50,
    alpha_merge=0.05
)
