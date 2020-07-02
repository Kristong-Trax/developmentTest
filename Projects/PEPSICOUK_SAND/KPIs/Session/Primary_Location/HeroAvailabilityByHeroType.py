from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from KPIUtils_v2.Utils.Consts.DB import StaticKpis, SessionResultsConsts
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd


class HeroSKUAvailabilityByHeroTypeKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroSKUAvailabilityByHeroTypeKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def calculate(self):
        lvl3_ass_res_df = self.dependencies_data
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY_BY_HERO_TYPE)
        if not lvl3_ass_res_df.empty:
            location_type_fk = self.util.scif[self.util.scif[ScifConsts.LOCATION_TYPE] == 'Primary Shelf']\
                [ScifConsts.LOCATION_TYPE_FK].values[0]
            product_hero_df = self.util.all_products[[ScifConsts.PRODUCT_FK, self.util.HERO_SKU_LABEL]]
            lvl3_ass_res_df = lvl3_ass_res_df.merge(product_hero_df, left_on='numerator_id',
                                                    right_on=ScifConsts.PRODUCT_FK,
                                                    how='left')
            lvl3_ass_res_df = lvl3_ass_res_df.merge(self.util.hero_type_custom_entity_df,
                                                    left_on=self.util.HERO_SKU_LABEL, right_on='name', how='left')
            lvl3_ass_res_df['count'] = 1
            kpi_res_df = lvl3_ass_res_df.groupby([self.util.HERO_SKU_LABEL, 'entity_fk'],
                                                 as_index=False).agg({'numerator_result': np.sum, 'count': np.sum})
            kpi_res_df['result'] = kpi_res_df['numerator_result'] / kpi_res_df['count'] * 100
            kpi_res_df['score'] = kpi_res_df['result'].apply(lambda x: 100 if x >= 100 else 0)
            for i, res in kpi_res_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=res['entity_fk'],
                                        numerator_result=res['numerator_result'], result=res['result'],
                                        denominator_id=res['entity_fk'], denominator_result=res['count'],
                                        score=res['score'], context_id=location_type_fk)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, res['entity_fk'], res['entity_fk'], res['result'], res['score'], location_type_fk])

    def kpi_type(self):
        pass
