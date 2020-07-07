from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts


class ShareOfAssortmentByHeroTypeKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(ShareOfAssortmentByHeroTypeKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def calculate(self):
        lvl3_ass_res_df = self.dependencies_data
        if not lvl3_ass_res_df.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.SHARE_OF_ASSORTMENT_BY_HERO_TYPE)
            in_store_skus = len(self.util.get_available_hero_sku_list(self.dependencies_data))  # total recognized
            location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf']\
                [ScifConsts.LOCATION_TYPE_FK].values[0]

            product_hero_df = self.util.all_products[[ScifConsts.PRODUCT_FK, self.util.HERO_SKU_LABEL]]
            lvl3_ass_res_df = lvl3_ass_res_df.merge(product_hero_df, left_on='numerator_id',
                                                    right_on=ScifConsts.PRODUCT_FK,
                                                    how='left')
            lvl3_ass_res_df = lvl3_ass_res_df.merge(self.util.hero_type_custom_entity_df,
                                                    left_on= self.util.HERO_SKU_LABEL, right_on='name', how='left')

            kpi_res_df = lvl3_ass_res_df.groupby([self.util.HERO_SKU_LABEL, 'entity_fk'],
                                                 as_index=False).agg({'numerator_result': np.sum})
            kpi_res_df['in_store_total'] = in_store_skus
            kpi_res_df['result'] = kpi_res_df.apply(self.get_result, axis=1)
            kpi_res_df['score'] = kpi_res_df['result'].apply(lambda x: 100 if x >= 100 else 0)
            for i, res in kpi_res_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=res['entity_fk'],
                                        numerator_result=res['numerator_result'], result=res['result'],
                                        denominator_id=self.util.store_id, denominator_result=res['in_store_total'],
                                        score=res['score'], context_id=location_type_fk)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, res['entity_fk'], res['entity_fk'], res['result'], res['score'], location_type_fk])

    def kpi_type(self):
        pass

    @staticmethod
    def get_result(row):
        rv = float(row['numerator_result']) / row['in_store_total'] * 100 if row['in_store_total'] else 0
        return rv

