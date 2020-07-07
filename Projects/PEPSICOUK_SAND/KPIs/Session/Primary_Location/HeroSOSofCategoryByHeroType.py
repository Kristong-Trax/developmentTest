from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class HeroSOSofCategoryByHeroTypeKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroSOSofCategoryByHeroTypeKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        hero_sos_kpi_results = self.dependencies_data
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_SOS_OF_CAT_BY_HERO_TYPE)
        if not hero_sos_kpi_results.empty:
            category_len_df = hero_sos_kpi_results.drop_duplicates(subset=['denominator_id'])
            category_len_df = category_len_df[['denominator_id', 'denominator_result']]

            location_type_fk = self.util.scif[self.util.scif[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
                [ScifConsts.LOCATION_TYPE_FK].values[0]
            product_hero_df = self.util.all_products[[ScifConsts.PRODUCT_FK, self.util.HERO_SKU_LABEL]]
            hero_sos_kpi_results = hero_sos_kpi_results.merge(product_hero_df, left_on='numerator_id',
                                                              right_on=ScifConsts.PRODUCT_FK,
                                                              how='left')
            hero_sos_kpi_results = hero_sos_kpi_results.merge(self.util.hero_type_custom_entity_df,
                                                              left_on=self.util.HERO_SKU_LABEL, right_on='name',
                                                              how='left')

            hero_type_by_cat = hero_sos_kpi_results.groupby([self.util.HERO_SKU_LABEL, 'entity_fk',
                                                             'denominator_id'], as_index=False).\
                agg({'numerator_result': np.sum})
            hero_type_by_cat = hero_type_by_cat.merge(category_len_df, on='denominator_id', how='left')
            hero_type_by_cat['sos'] = hero_type_by_cat['numerator_result'] / hero_type_by_cat['denominator_result'] * 100
            hero_type_by_cat['score'] = hero_type_by_cat['sos'].apply(lambda x: 100 if x >= 100 else 0)

            for i, res in hero_type_by_cat.iterrows():
                self.write_to_db_result(fk=kpi_fk, score=res['score'], result=res['sos'], numerator_id=res['entity_fk'],
                                        denominator_id=res['denominator_id'], numerator_result=res['numerator_result'],
                                        denominator_result=res['denominator_result'], context_id=location_type_fk)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, res['entity_fk'], res['denominator_id'], res['sos'], res['score'], location_type_fk])