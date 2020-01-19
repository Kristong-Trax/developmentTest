import pandas as pd
import json
import math
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


class LiveAssortmentDataProvider(PSAssortmentDataProvider):
    def __init__(self, data_provider):
        PSAssortmentDataProvider.__init__(self, data_provider)

    def get_policies(self, policy_name=None):
        policy_sql = "select pk, policy, policy_type from pservice.policy"
        if policy_name:
            policy_sql = policy_sql + " where policy_name = '{}'".format(policy_name)
        policies_df1 = self.pandas_read_sql_query(policy_sql)
        list = []
        for row in policies_df1.itertuples():
            list.append(json.loads(row.policy))
        policies_df = pd.DataFrame(list)
        store_df_1 = self.data_provider.store_info
        store_df = PsDataProvider(self.data_provider, None).get_ps_store_info(store_df_1)
        store_dict = store_df.to_dict('records')[0]
        columns = policies_df.columns.values.tolist()
        final_df = policies_df1.merge(policies_df, left_index=True, right_index=True, how='left')
        for column in columns:
            boolean_values = []
            for current_column_value in final_df[column].tolist():
                # check if this value is nan
                if type(current_column_value) == float and math.isnan(current_column_value):
                    boolean_values.append(True)
                else:
                    # convert column name and current value to lowercase for comparision
                    lower_column = unicode(store_dict.get(column)).lower()
                    lower_current_value = map(lambda a: a.lower(), current_column_value)
                    if lower_column in lower_current_value:
                        boolean_values.append(True)
                    else:
                        boolean_values.append(False)
            col_series = pd.Series(boolean_values)
            col_series._set_name(column, True)
            final_df = final_df[col_series]
            final_df.reset_index(drop=True, inplace=True)
        final_df.dropna('index', 'all', inplace=True)
        if final_df is None or final_df.empty:
            raise Exception('No assortment policies were found the requested session')

        final_df = final_df[['pk', 'policy', 'policy_type']]
        return final_df

