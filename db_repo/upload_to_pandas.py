import pandas as pd

from configs.mongodb import terrorism_actions


def upload_to_pandas():
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
    except Exception as e:
        return 'Could not connect to database or to convert to pandas dataframe'
    finally:
        return df