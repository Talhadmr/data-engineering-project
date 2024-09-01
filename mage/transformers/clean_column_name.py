from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    df = pd.DataFrame(df)
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame")
    cols = list(df.columns)
    print("Column names before formatting: ", cols)
    
    action = build_transformer_action(
        df,
        action_type=ActionType.CLEAN_COLUMN_NAME,
        arguments=df.columns,
        axis=Axis.COLUMN,
    )
    
    df = BaseAction(action).execute(df)
    cols = list(df.columns)
    print("Column names after formatting: ", cols)
    
    return df 


