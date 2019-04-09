import pandas as pd
import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Utils.Logging.Logger import Log
from Projects.MONDELEZUS.CStore.Const import Const

__author__ = 'sam'


class CSTOREToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scene_results = self.data_provider[Data.SCENE_KPI_RESULTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif['store_fk'] = self.store_id
        self.scif = self.scif[~(self.scif['product_type'].isin([Const.IRRELEVANT, Const.EMPTY]))]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpis = self.load_kpis()
        self.results_values = self.load_results_values()
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])

        self.kpi_results = []

        self.assortment = Assortment(self.data_provider, self.output)


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # if not self.filter_df(self.scif, self.brand_filter).empty:
        self.calculate_displays()
        self.calculate_assortment()

        for result in self.kpi_results:
            self.write_to_db(**result)
        return

    def calculate_facings_sos(self):
        self.safety_func('Facings SOS', self.calculate_sos, [Const.FACING_SOS_KPI, {}])

    def safety_func(self, group, func, args):
        try:
            func(*args)
            Log.info('{} KPIs Calculated'.format(group))
        except Exception as e:
            Log.error('ERROR {} KPIs Failed to Calculate'.format(group))
            Log.error(e)

    def calculate_displays(self):
        relevant_kpis = self.kpis[self.kpis[Const.KPI_FAMILY] == Const.DISPLAY]
        relevant_kpis['num_types'] = self.name_to_col_name(relevant_kpis[Const.NUMERATOR])
        df_base = self.scif[self.scif['template_name'] == 'Displays - Mondelez Brands Only']
        df_base = df_base[df_base['manufacturer_fk'] == self.manufacturer_fk]
        df_base['numerator_result'], df_base['result'] = 1, 1
        if not df_base.empty:
            for i, kpi in relevant_kpis.iterrows():
                parent = relevant_kpis[relevant_kpis['type'] == Const.SOS_HIERARCHY[kpi['type']]] #  Note, parent is df, kpi is a series
                df = df_base.copy()
                df = self.update_and_rename_df(df, kpi, parent)
                df = self.transform_new_col(df, 'numerator_id', 'numerator_result')
                df.drop('scene_id', axis=1, inplace=True)
                df.drop_duplicates(inplace=True)
                df['result'] = df['numerator_result']
                if parent.empty:
                    df['denominator_id'] = self.store_id
                    df.drop('ident_parent', axis=1, inplace=True)
                self.update_results(df)
        else:
            df = pd.DataFrame([self.store_id], columns=['denominator_id'])
            df['numerator_id'] = self.manufacturer_fk
            df['numerator_result'], df['result'] = 0, 0
            df['kpi_name'] = [key for key, val in Const.SOS_HIERARCHY.items() if val == 'ihavenoparent'][0]
            self.update_results(df)

    def update_results(self, df):
        results = [val for key, val in df.to_dict('index').items()]
        self.kpi_results += results

    def update_and_rename_df(self, df, kpi, parent):
        df['ident_result'] = ['{}_{}'.format(row[kpi['num_types']], kpi['type'])
                              for i, row in df.iterrows()]
        df['ident_parent'] = ['{}_{}_{}'.format(row[kpi['num_types']], 'Parent', kpi['type'])
                              for i, row in df.iterrows()]
        parent_col = ['ident_parent']
        if not parent.empty:
            df['ident_parent'] = ['{}_{}'.format(row[parent['num_types'].iloc[0]], parent['type'].iloc[0])
                                  for i, row in df.iterrows()]  #parent is a df, hence the iloc
        df = df[['scene_id', 'numerator_result', 'result', kpi['num_types'], 'ident_result'] + parent_col]
        df.drop_duplicates(inplace=True)
        df.rename(columns={kpi['num_types']: 'numerator_id'}, inplace=True)
        df['kpi_name'] = kpi['type']
        return df

    def name_to_col_name(self, col):
        return ['{}_fk'.format(num) for num in col]

    def transform_new_col(self, df, group_col, sum_col):
        df[sum_col] = df.groupby(group_col)[sum_col].transform('sum')
        return df

    def grouper(self, filter, df):
        return self.filter_df(df, filter).groupby(filter.keys()[0])

    def calculate_assortment(self):
        lvl3_results = self.assortment.calculate_lvl3_assortment()
        if not lvl3_results.empty:
            for kpi in lvl3_results['kpi_fk_lvl3'].unique():
                lvl3_result = lvl3_results[lvl3_results['kpi_fk_lvl3']==kpi]
                lvl3_result['target'] = 1
                lvlx_result = pd.DataFrame()
                lvl1_result = pd.DataFrame()

                # For Dist, they have assortments, but want the results by category
                # and since there is only one policy per store (barring new which is
                # handled elsewhere) we will kust pretend that category_fk is the
                # level 2 assortment group.  God rest the soul of whomever needs
                # to implement additional policies.
                if kpi == 4000:
                    lvl3_result = lvl3_result.set_index('product_fk').join(self.all_products.set_index('product_fk')
                                                                           ['category_fk']).reset_index()\
                                                                            .drop_duplicates()
                    lvl3_result = lvl3_result.rename(columns={'assortment_group_fk': 'ass_grp_fk',
                                                              'category_fk': 'assortment_group_fk'})
                    lvlx_result = self.assortment_additional(lvl3_result)

                lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
                if not lvl2_result['kpi_fk_lvl1'].any():
                    lvl3_result['assortment_group_fk'] = self.manufacturer_fk
                    lvl2_result['assortment_group_fk'] = self.manufacturer_fk
                    lvl2_result['assortment_super_group_fk'] = self.store_id
                else:
                    lvl1_result = self.assortment.calculate_lvl1_assortment(lvl2_result)
                    lvl1_result['total'] = lvl2_result['total'].sum()
                    lvl1_result['passes'] = lvl2_result['passes'].sum()
                    lvl1_result['num_id'] = self.manufacturer_fk
                    lvl1_result['den_id'] = self.store_id

                lvl3_result['in_store'] = lvl3_result['in_store'].apply(lambda x:
                                                                        self.results_values[Const.RESULTS_TYPE_DICT[x]])

                self.parse_assortment_results(lvl3_result, 'kpi_fk_lvl3', 'product_fk', 'in_store', 'assortment_group_fk',
                                              'target', None, 'assortment_group_fk')
                self.parse_assortment_results(lvlx_result, 'kpi_fk_lvl3', 'product_fk', 'in_store', 'assortment_group_fk',
                                              'target', None, 'assortment_group_fk')
                self.parse_assortment_results(lvl2_result, 'kpi_fk_lvl2', 'assortment_group_fk', 'passes', 'assortment_fk',
                                              'total', 'assortment_group_fk', 'assortment_super_group_fk')
                self.parse_assortment_results(lvl1_result, 'kpi_fk_lvl1', 'num_id', 'passes',
                                              'den_id', 'total', 'assortment_super_group_fk', None)
                self.assortment.LVL2_HEADERS += ['passes', 'total']
                self.assortment.LVL1_HEADERS += ['passes', 'total']

            Log.info('Assortment KPIs Calculated')

    def parse_assortment_results(self, df, kpi_col, num_id_col, num_col, den_id_col, den_col, self_id, parent):
        for i, row in df.iterrows():
            kpi_res = {'kpi_fk': row[kpi_col],
                       'numerator_id': row[num_id_col],
                       'numerator_result': row[num_col],
                       'denominator_id': row[den_id_col],
                       'denominator_result': row[den_col],
                       'score': self.safe_divide(row[num_col], row[den_col]),
                       'result': self.safe_divide(row[num_col], row[den_col]),
                       'ident_result': row[self_id] if self_id else None,
                       'ident_parent': row[parent] if parent else None}
            self.kpi_results.append(kpi_res)

    def assortment_additional(self, lvl3_result):
        assort = set(lvl3_result['product_fk'])
        additional_sku_df = self.scif[~self.scif['product_fk'].isin(assort)]
        additional_sku_df = additional_sku_df[additional_sku_df['manufacturer_fk'] == self.manufacturer_fk]
        additional_sku_df = additional_sku_df[additional_sku_df['facings'] != 0][['product_fk', 'category_fk']]
        additional_sku_df['kpi_fk_lvl3'] = lvl3_result['kpi_fk_lvl3'].values[0]
        additional_sku_df['in_store'] = self.results_values['Additional']
        additional_sku_df['target'] = 0
        additional_sku_df = additional_sku_df.rename(columns={'category_fk': 'assortment_group_fk'})
        return additional_sku_df

    def safe_divide(self, num, den):
        res = num
        if num <= den:
            res = round((float(num) / den) * 100, 2) if num and den else 0
            res = '{:.2f}'.format(res)
        return res

    @staticmethod
    def filter_df(df, filters, exclude=0):
        cols = set(df.columns)
        for key, val in filters.items():
            if key not in cols:
                return pd.DataFrame()
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def load_kpis(self):
        return pd.read_sql_query(Const.KPI_QUERY, self.rds_conn.db)

    def load_results_values(self):
        return pd.read_sql_query(Const.RESULT_TYPE_QUERY, self.rds_conn.db).set_index('value')['pk'].to_dict()


    def write_to_db(self, kpi_name=None, score=0, result=None, target=None, numerator_result=None, scene_result_fk=None,
                    denominator_result=None, numerator_id=999, denominator_id=999, ident_result=None, ident_parent=None,
                    kpi_fk = None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        if not kpi_fk and kpi_name:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id,
                                       identifier_result=ident_result, identifier_parent=ident_parent,
                                       scene_result_fk=scene_result_fk)

