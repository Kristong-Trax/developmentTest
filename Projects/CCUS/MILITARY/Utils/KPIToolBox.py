from __future__ import division

# from KPIUtils_v2.Utils.Consts.DataProvider import
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCUS.MILITARY.Utils import Const

__author__ = 'trevaris'


class MilitaryToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.session_uid = self.data_provider.session_uid
        self.own_manufacturer = self.data_provider[Data.OWN_MANUFACTURER].iloc[0]['param_value']
        self.match_product_in_scene = data_provider[Data.MATCHES]
        self.products = data_provider[Data.PRODUCTS]
        self.scene_info = data_provider[Data.SCENES_INFO]
        self.store_info = data_provider[Data.STORE_INFO]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.products = self.data_provider[Data.ALL_PRODUCTS]

        self.mpis = self.match_product_in_scene \
            .merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s'])

    def main_calculation(self):
        if self.filter_df(self.store_info, {'region_name': Const.REGION}).empty:
            return

        self.calculate_compliant_bay_kpi()

    def calculate_compliant_bay_kpi(self):
        for kpi in Const.KPIs[Const.COMPLIANT_BAY_COUNT]:
            self.calculate_compliant_bay(
                kpi['name'], kpi['template'], kpi['manufacturers'], kpi['exclude_manufacturers'])

    def calculate_compliant_bay(self, kpi, templates, manufacturers, exclude_manufacturers=False, sos_threshold=.501):
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        template_ids = self.get_template_fks(templates)
        manufacturer_ids = self.get_manufacturer_fks(manufacturers)

        filtered_df = self.filter_df(self.mpis, filters={'template_fk': template_ids})
        
        if filtered_df.empty:
            return
        
        filtered_df['count'] = 1
        filtered_df = filtered_df.groupby(['scene_fk', 'bay_number']).count().reset_index()

        def judge_compliance(row):
            target_count = len(self.filter_df(self.mpis,
                                              filters={
                                                  'scene_fk': row['scene_fk'],
                                                  'bay_number': row['bay_number'],
                                                  'manufacturer_fk': manufacturer_ids
                                              }))
            return 1 if target_count / row['count'] >= 0.501 else 0

        compliant_bays = filtered_df.apply(judge_compliance, axis=1)
        result = sum(compliant_bays)

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=self.own_manufacturer,
            numerator_result=result,
            denominator_id=self.store_info.iloc[0]['store_fk'],
            denominator_result=1,
            result=result
        )

    def get_template_fks(self, template_names):
        if not isinstance(template_names, list):
            template_names = [template_names]
        return self.templates[self.templates['template_name'].isin(template_names)]['template_fk']  # unique?

    def get_manufacturer_fks(self, manufacturer_names):
        if not isinstance(manufacturer_names, list):
            manufacturer_names = [manufacturer_names]
        return self.products[self.products['manufacturer_name'].isin(manufacturer_names)]['manufacturer_fk'].unique()

    @staticmethod
    def filter_df(df, filters, exclude=False):
        for key, val in filters.items():
            if not hasattr(val, '__iter__'):
                val = [val]

            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def commit_results(self):
        self.common.commit_results_data()
