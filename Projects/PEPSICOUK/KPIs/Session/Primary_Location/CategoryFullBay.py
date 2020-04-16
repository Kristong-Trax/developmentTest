from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, ProductsConsts, MatchesConsts
import numpy as np


class CategoryFullBayKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(CategoryFullBayKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.CATEGORY_FULL_BAY)
        category_fk = self.util.all_products[self.util.all_products[ProductsConsts.CATEGORY] == self.util.CSN]\
            [ProductsConsts.CATEGORY_FK].values[0]
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.CATEGORY_FULL_BAY)
        filtered_matches = self.util.filtered_matches[~(self.util.filtered_matches[MatchesConsts.BAY_NUMBER] == -1)]
        if not filtered_matches.empty:
            scene_bay_product = filtered_matches.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER,
                                                          ScifConsts.PRODUCT_FK], as_index=False).agg({'count': np.sum})
            scene_bay_product = scene_bay_product.merge(self.util.all_products, on=ProductsConsts.PRODUCT_FK, how='left')
            scene_bay = scene_bay_product.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER],
                                                  as_index=False).agg({'count': np.sum})
            scene_bay.rename(columns={'count': 'total_facings'}, inplace=True)

            cat_relevant_df = scene_bay_product[scene_bay_product[ProductsConsts.CATEGORY_FK] == category_fk]
            result_df = cat_relevant_df.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER],
                                                as_index=False).agg({'count': np.sum})
            result_df = result_df.merge(scene_bay, on=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER], how='left')
            result_df['ratio'] = result_df['count'] / result_df['total_facings']
            target_ratio = float(self._config_params['ratio'])
            result = len(result_df[result_df['ratio'] >= target_ratio])
            self.write_to_db_result(fk=kpi_fk, numerator_id=category_fk, denominator_id=self.util.store_id,
                                    score=result, result=result)
            self.util.add_kpi_result_to_kpi_results_df([kpi_fk, category_fk, self.util.store_id, result, result, None])

        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()