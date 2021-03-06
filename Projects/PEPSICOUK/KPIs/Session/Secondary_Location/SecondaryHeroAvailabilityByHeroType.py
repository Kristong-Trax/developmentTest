from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from Trax.Data.ProfessionalServices.PsConsts.DB import StaticKpis, SessionResultsConsts
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd


class HeroSKUAvailabilityByHeroTypeKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroSKUAvailabilityByHeroTypeKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        if self.util.commontools.are_all_bins_tagged:
            total_skus_in_ass = len(self.util.lvl3_ass_result)
            if not total_skus_in_ass:
                return
            lvl3_ass_res_df = self.dependencies_data
            if not lvl3_ass_res_df.empty:
                total_available_products = len(lvl3_ass_res_df)
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
                product_hero_df = self.util.all_products[[ScifConsts.PRODUCT_FK, self.util.HERO_SKU_LABEL]]
                lvl3_ass_res_df = lvl3_ass_res_df.merge(product_hero_df, left_on='numerator_id',
                                                        right_on=ScifConsts.PRODUCT_FK,
                                                        how='left')
                lvl3_ass_res_df = lvl3_ass_res_df.merge(self.util.hero_type_custom_entity_df,
                                                        left_on=self.util.HERO_SKU_LABEL, right_on='name', how='left')
                # lvl3_ass_res_df['count'] = 1
                kpi_res_df = lvl3_ass_res_df.groupby([self.util.HERO_SKU_LABEL, 'entity_fk'],
                                                     as_index=False).agg({'numerator_result': np.sum})
                kpi_res_df['result'] = kpi_res_df['numerator_result'] / total_available_products * 100
                kpi_res_df['score'] = kpi_res_df['result'].apply(lambda x: 100 if x >= 100 else 0)
                for i, res in kpi_res_df.iterrows():
                    self.write_to_db_result(fk=kpi_fk, numerator_id=res['entity_fk'],
                                            numerator_result=res['numerator_result'], result=res['result'],
                                            denominator_id=res['entity_fk'], denominator_result=total_available_products,
                                            score=res['score'])

    def kpi_type(self):
        pass
