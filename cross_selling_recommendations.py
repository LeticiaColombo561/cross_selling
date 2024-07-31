import json

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from datetime import datetime

import pandas as pd
import pandas_gbq
import os

from cs.config import PROJECT_ID
from cs.config import DATA_SET
from cs.config import MIN_THRESHOLD
from cs.config import METRIC
from cs.config import FROM_DATE
from cs.config import TO_DATE
from cs.config import ENV

from cs.clean_insert_data_bq import groceries_dataset, save_ranking_top_products
from cs.clean_insert_data_bq import save_recommendation_historical, delete_historical_recommendation_today
from cs.clean_insert_data_bq import save_all_recommendations_mba, export_sns_mba
from cs.clean_insert_data_bq import save_all_recommendations_ranking, export_sns_ranking
from cs.export_to_sns import export_to_sns, get_gcp_credentials, CustomJSONEncoder
from cs.fp_growth_algorithms import compute_association_rule, perform_rule_calculation
from mlxtend.preprocessing import TransactionEncoder

print('Actual environment project:', ENV)

"Bigquery connection"
credential_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if not credential_path:
    print('Path error!')

credentials = Credentials.from_service_account_file(credential_path)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID,
)

"Generate dataset"
groceries = client.query(groceries_dataset).to_dataframe()

"Compute FP Growth Rule"
all_recommendations_by_country = pd.DataFrame()

for country in list(groceries.country_id.unique()):
    print(country)

    "Generate transactions list per country_id"
    all_transactions = [transaction[1]['gtin'].tolist()
                        for transaction
                        in list(groceries.loc[groceries.country_id == country].groupby(['order_id']))]

    "Make transaction encoder matrix"
    trans_encoder = TransactionEncoder()
    trans_encoder_matrix = trans_encoder.fit(all_transactions).transform(all_transactions,
                                                                         sparse=True)
    trans_encoder_matrix = pd.DataFrame.sparse.from_spmatrix(trans_encoder_matrix,
                                                             columns=trans_encoder.columns_)

    "Make rule calculation rule"
    fpgrowth_matrix, fp_growth_exec_time = perform_rule_calculation(trans_encoder_matrix)

    "Perform lift result"
    fp_growth_rule_lift = compute_association_rule(fpgrowth_matrix,
                                                   metric=METRIC,
                                                   min_thresh=MIN_THRESHOLD)

    result_fpg_rule_lift = fp_growth_rule_lift.explode('antecedents',
                                                       ignore_index=True).explode('consequents'
                                                                                  ).copy()

    "Save results recommend products"
    recommendation_by_country = result_fpg_rule_lift[['antecedents',
                                                      'consequents',
                                                      'support',
                                                      'confidence',
                                                      'lift'
                                                      ]]
    recommendation_by_country['country_id'] = country

    recommendation_by_country = recommendation_by_country[['country_id',
                                                           'antecedents',
                                                           'consequents',
                                                           'support',
                                                           'confidence',
                                                           'lift'
                                                           ]].groupby(['country_id',
                                                                       'antecedents',
                                                                       'consequents',
                                                                       'support',
                                                                       'confidence'
                                                                       ]).max().reset_index()

    all_recommendations_by_country = pd.concat([all_recommendations_by_country,
                                                recommendation_by_country],
                                               ignore_index=True
                                               )

if not all_recommendations_by_country.empty:
    pandas_gbq.to_gbq(all_recommendations_by_country,
                      str(DATA_SET) + '.' + 'recommendation_by_country',
                      project_id=PROJECT_ID,
                      if_exists='replace'
                      )

"Save Recommendation Historical"
t = datetime.now().strftime("%Y-%m-%d")
client.query(
    delete_historical_recommendation_today(PROJECT_ID,
                                           DATA_SET,
                                           t)
).result()

client.query(
    save_recommendation_historical(PROJECT_ID,
                                   DATA_SET,
                                   t)
).result()

"Save Ranking Top Products"
ranking_top_products = client.query(
    save_ranking_top_products(PROJECT_ID,
                              DATA_SET,
                              FROM_DATE,
                              TO_DATE)
).to_dataframe()

"Export to sns"
"MBA"
all_recommendations_mba = client.query(
    save_all_recommendations_mba(PROJECT_ID,
                                 DATA_SET,
                                 METRIC
                                 )
).result()

sns_data_mba = client.query(
    export_sns_mba(PROJECT_ID,
                   DATA_SET)
).to_dataframe().to_dict(orient="records")

"Ranking"
all_recommendations_ranking = client.query(
    save_all_recommendations_ranking(PROJECT_ID,
                                     DATA_SET
                                     )
).result()

sns_data_ranking = client.query(
    export_sns_ranking(PROJECT_ID,
                       DATA_SET)
).to_dataframe().to_dict(orient="records")

"Json format"
sns_json_data_mba = json.loads(json.dumps(sns_data_mba,
                                          ls=CustomJSONEncoder
                                          )
                               )

sns_json_data_ranking = json.loads(json.dumps(sns_data_ranking,
                                              cls=CustomJSONEncoder
                                              )
                                   )

"Send to SNS"
os.environ['GOOGLE_APPLICATION_CREDENTIALS']


with open(credential_path) as file:
    credentials = json.load(file)

export_to_sns(sns_json_data_mba, "mba", service_account_info=None, env=ENV)
export_to_sns(sns_json_data_ranking, "ranking", service_account_info=None, env=ENV)
