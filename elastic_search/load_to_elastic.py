import pandas as pd
from configs.mongodb import news
from elastic_search.indexing import es
from configs.database_url import DATA_PATH
from db_repo.upload_to_pandas import upload_to_pandas

data1 = pd.read_csv(DATA_PATH, low_memory=False)
data2 = upload_to_pandas(news)

data1 = data1.where(pd.notnull(data1), None)
data2 = data2.where(pd.notnull(data2), None)


def load_to_elasticsearch(dataframe, index_name, max_rows=50):
    dataframe = dataframe.head(max_rows)
    for _, row in dataframe.iterrows():
        doc = row.to_dict()

        if '_id' in doc:
            doc.pop('_id')

        if 'wounded' in doc:
            value = doc['wounded']
            if isinstance(value, str):
                if value.lower() in ['over 200', 'several', 'unknown']:
                    doc['wounded'] = None
                else:
                    try:
                        doc['wounded'] = int(value)
                    except ValueError:
                        doc['wounded'] = None

        for key, value in doc.items():
            if pd.isna(value) or value == "NaN":
                doc[key] = None

        try:
            es.index(index=index_name, body=doc)
        except Exception as e:
            print(f"Failed to index document: {e}")


load_to_elasticsearch(data1, "historic_data")
load_to_elasticsearch(data2, "realtime_news")