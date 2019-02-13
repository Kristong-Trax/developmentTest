import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Utils.Logging.Logger import Log
from Projects.GPUS.Utils.Const import Const

__author__ = 'sam, nicolaske'


class GPUSToolBox:

    def __init__(self, data_provider, common, output):
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
        self.mpis = self.make_mpis()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif['store_fk'] = self.store_id
        self.scif = self.scif[~(self.scif['product_type'] == Const.IRRELEVANT)]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpis = self.load_kpis()
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.dist_oos = self.dist_kpi_to_oos_kpi()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.gp_manufacturer = self.get_gp_manufacturer()
        self.gp_categories = self.get_gp_categories()
        self.gp_brands = self.get_gp_brands()
        self.all_man = self.scif[['manufacturer_name', 'manufacturer_fk']].set_index('manufacturer_name')\
                                                                          ['manufacturer_fk'].to_dict()
        self.all_brands = self.scif[['brand_name', 'brand_fk']].set_index('brand_name')['brand_fk'].to_dict()
        self.man_fk_filter = {'manufacturer_name': list(self.gp_manufacturer.keys())}
        self.cat_filter = {'category': list(self.gp_categories.keys())}
        self.brand_filter = {'brand_name': list(self.gp_brands.keys())}
        self.all_brands_filter = {'brand_name': list(self.all_brands.keys())}
        self.all_man_filter = {'manufacturer_name': list(self.all_man.keys())}
        self.kpi_results = []

        self.assortment = Assortment(self.data_provider, self.output)


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.filter_df(self.scif, self.brand_filter).empty:
            # self.calculate_facings_sos()
            # self.calculate_linear_sos()
            self.calculate_adjacency()
            # self.calculate_assortment()

        if not self.filter_df(self.scif, self.cat_filter).empty:
            # self.calculate_share_of_empty()
            pass

        for result in self.kpi_results:
            self.write_to_db(**result)
        return

    def calculate_facings_sos(self):
        self.safety_func('Facings SOS', self.calculate_sos, [Const.FACING_SOS_KPI, {}])

    def calculate_linear_sos(self):
        self.safety_func('Linear SOS', self.calculate_sos, [Const.LINEAR_SOS_KPI, {}])

    def calculate_share_of_empty(self):
        self.safety_func('Share of Empty', self.calculate_sos, [Const.SHARE_OF_EMPTY_KPI, {'numerator_id': 0}])

    def calculate_adjacency(self):
        self.safety_func('Adjacency', self.update_adjacency, [])

    def safety_func(self, group, func, args):
        try:
            func(*args)
            Log.info('{} KPIs Calculated'.format(group))
        except Exception as e:
            Log.error('ERROR {} KPIs Failed to Calculate'.format(group))
            Log.error(e)

    def update_adjacency(self):
        kpis = self.kpis[self.kpis['type'].str.contains(r'^(?=.*Total)(?=.*Adjacencies)')]
        parent_kpi = kpis[kpis[Const.KPI_FAMILY] == Const.PS_PARENT].iloc[0]
        columns = ['numerator_result', 'denominator_result', 'kpi_name', 'ident_result', 'ident_parent']
        sub_columns = ['kpi_fk', 'scene_result_fk', 'ident_parent', 'hierarchy_only']
        results = pd.DataFrame([[None]*len(columns)]*(len(Const.EDGES)+1), columns=columns)
        for i, dir in enumerate(Const.EDGES):
            ident = '{}_Parent'.format(dir)
            kpi = kpis[kpis['type'].str.contains('(?i){}'.format(dir))].iloc[0]
            df = self.scene_results[self.scene_results['type'].str.contains('(?i){}'.format(dir))]
            df['hierarchy_only'] = 1
            df.rename(columns={'scene_kpi_fk': 'scene_result_fk'}, inplace=True)
            cat_res = df[df['type'].str.contains('(?i){}'.format('category'))]
            brand_res = df[df['type'].str.contains('(?i){}'.format('brand'))]
            cat_res['kpi_fk'] = 0
            cat_res['ident_parent'] = ident
            self.kpi_results += [v for k, v in cat_res[sub_columns].to_dict('index').items()]

            results.loc[i, 'numerator_result'] = brand_res.shape[0]
            results.loc[i, 'denominator_result'] = cat_res.shape[0]
            results.loc[i, 'kpi_name'] = kpi['type']
            results.loc[i, 'ident_result'] = ident
            results.loc[i, 'ident_parent'] = 'Adjacency_parent'

        results.loc[i+1, :] = results.sum()
        results.loc[i+1, 'kpi_name'] = parent_kpi['type']
        results.loc[i+1, 'ident_result'] = 'Adjacency_parent'
        results[results.isnull()] = None
        self.kpi_results += [v for k, v in results.to_dict('index').items()]

    def calculate_sos(self, kpi_family, kpi_filter):
        relevant_kpis = self.kpis[self.kpis[Const.KPI_FAMILY] == kpi_family]
        sum_col = Const.SUM_COLS[kpi_family]
        relevant_kpis['num_types'] = self.name_to_col_name(relevant_kpis[Const.NUMERATOR])
        relevant_kpis['den_types'] = self.name_to_col_name(relevant_kpis[Const.DENOMINATOR])

        for i, kpi in relevant_kpis.iterrows():
            parent = relevant_kpis[relevant_kpis['type'] == Const.SOS_HIERARCHY[kpi['type']]] #  Note, parent is df, kpi is a series
            df = self.scif.copy()
            df = self.transform_new_col(df, [kpi['num_types'], kpi['den_types']], sum_col, 'numerator_result')
            df = self.transform_new_col(df, kpi['den_types'], sum_col, 'denominator_result')
            df['result'] = df['numerator_result'].astype(float) / df['denominator_result']
            df = self.update_and_rename_df(df, kpi, parent)
            self.update_results(df, kpi_filter)
            if parent.empty:
                df['denominator_id'] = self.store_id
                df.drop('ident_result', axis=1, inplace=True)
                df.rename(columns={'ident_parent': 'ident_result'}, inplace=True)
                df['kpi_name'] = Const.SOS_HIERARCHY[kpi['type']]
                self.update_results(df, {'numerator_id': self.manufacturer_fk})

    def update_results(self, df, filter):
        df = self.filter_df(df, filter)
        results = [val for key, val in df.to_dict('index').items()]
        self.kpi_results += results

    def update_and_rename_df(self, df, kpi, parent):
        df['ident_result'] = ['{}_{}_{}'.format(row[kpi['num_types']], row[kpi['den_types']], kpi['type'])
                              for i, row in df.iterrows()]
        df['ident_parent'] = ['{}_{}_{}_{}'.format(row[kpi['num_types']], row[kpi['den_types']], 'Parent', kpi['type'])
                              for i, row in df.iterrows()]
        parent_col = ['ident_parent']
        if not parent.empty:
            df['ident_parent'] = ['{}_{}_{}'.format(row[parent['num_types'].iloc[0]], row[parent['den_types'].iloc[0]],
                                                    parent['type'].iloc[0]) for i, row in df.iterrows()]  #parent is a df, hence the iloc
        df = df[['numerator_result', 'denominator_result', 'result', kpi['den_types'], kpi['num_types'],
                 'ident_result'] + parent_col]
        df.drop_duplicates(inplace=True)
        df.rename(columns={kpi['num_types']: 'numerator_id', kpi['den_types']: 'denominator_id'}, inplace=True)
        df['kpi_name'] = kpi['type']
        return df

    def name_to_col_name(self, col):
        return ['{}_fk'.format(num) for num in col]

    def transform_new_col(self, df, group_col, sum_col, name):
        df[name] = df.groupby(group_col)[sum_col].transform('sum')
        return df

    def grouper(self, filter, df):
        return self.filter_df(df, filter).groupby(filter.keys()[0])

    def calculate_assortment(self):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            lvl3_result['target'] = 1

            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            lvl1_result = self.assortment.calculate_lvl1_assortment(lvl2_result)
            lvl1_result['total'] = lvl2_result['total'].sum()
            lvl1_result['passes'] = lvl2_result['passes'].sum()
            lvl1_result['num_id'] = self.manufacturer_fk
            lvl1_result['den_id'] = self.store_id

            self.parse_assortment_results(lvl3_result, 'kpi_fk_lvl3', 'product_fk', 'in_store', 'assortment_fk',
                                          'target', None, 'assortment_group_fk')
            self.parse_assortment_results(lvl2_result, 'kpi_fk_lvl2', 'assortment_group_fk', 'passes', 'assortment_fk',
                                          'total', 'assortment_group_fk', 'assortment_super_group_fk')
            self.parse_assortment_results(lvl1_result, 'kpi_fk_lvl1', 'num_id', 'passes',
                                          'den_id', 'total', 'assortment_super_group_fk', None)
        Log.info('Assortment KPIs Calculated')

    def parse_assortment_results(self, df, kpi_col, num_id_col, num_col, den_id_col, den_col, self_id, parent):
        for i, row in df.iterrows():
            kpi_res = {'kpi_fk': row[kpi_col],
                       'numerator_id': row[num_id_col],
                       'numerator_result': row[num_col],
                       'denominator_id': row[den_id_col],
                       'denominator_result': row[den_col],
                       'score': 1,
                       'result': self.safe_divide(row[num_col], row[den_col]),
                       'ident_result': row[self_id] if self_id else None,
                       'ident_parent': row[parent] if parent else None}
            self.kpi_results.append(kpi_res)
            oos_res = dict(kpi_res)
            oos_res.update({
                            'kpi_fk': self.dist_oos[row[kpi_col]],
                            'numerator_result': row[den_col] - row[num_col],
                            'denominator_result':0,
                            'result': self.safe_divide((row[den_col] - row[num_col]), row[den_col]),
                            'ident_result': 'oos_{}'.format(row[self_id]) if self_id else None,
                            'ident_parent': 'oos_{}'.format(row[parent]) if parent else None,
                            })
            self.kpi_results.append(oos_res)

    def safe_divide(self, num, den):
        return (float(num) / den) * 100 if num else 0

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

    def get_gp_categories(self):
        cats = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['category', 'category_fk']]\
                              .drop_duplicates().set_index('category')['category_fk'].to_dict()
        return cats

    def get_gp_brands(self):
        brands = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['brand_name', 'brand_fk']]\
                              .drop_duplicates().set_index('brand_name')['brand_fk'].to_dict()
        return brands

    def get_gp_manufacturer(self):
        name = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk].reset_index()
        name = '' if name.empty else name.loc[0, 'manufacturer_name']
        return {name: self.manufacturer_fk}

    def make_mpis(self):
        mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        return mpis

    def dist_kpi_to_oos_kpi(self):
        dist = self.kpis[(self.kpis[Const.KPI_FAMILY] == Const.ASSORTMENT)&(self.kpis['kpi_calculation_stage_fk'] == 3)]
        oos = self.kpis[(self.kpis[Const.KPI_FAMILY] == Const.OOS) & (self.kpis['kpi_calculation_stage_fk'] == 3)]
        return {
                dist[dist['type'].str.contains('SKU')].pk.iloc[0]: oos[oos['type'].str.contains('SKU')].pk.iloc[0],
                dist[dist['type'].str.contains('Tota')].pk.iloc[0]: oos[oos['type'].str.contains('Tota')].pk.iloc[0],
                dist[~dist['type'].str.contains(' ')].pk.iloc[0]: oos[~oos['type'].str.contains(' ')].pk.iloc[0],
                }

    def load_kpis(self):
        return pd.read_sql_query(Const.KPI_QUERY, self.rds_conn.db)

    def write_to_db(self, kpi_name=None, score=0, result=None, target=None, numerator_result=None, scene_result_fk=None,
                    denominator_result=None, numerator_id=999, denominator_id=999, ident_result=None, ident_parent=None,
                    kpi_fk = None, hierarchy_only=0):
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
                                       scene_result_fk=scene_result_fk, hierarchy_only=0)

