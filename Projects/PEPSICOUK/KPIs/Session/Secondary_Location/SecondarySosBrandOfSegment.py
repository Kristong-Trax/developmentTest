from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts
import pandas as pd
import numpy as np


class SecondarySosBrandOfSegmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondarySosBrandOfSegmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        if self.util.commontools.are_all_bins_tagged:
            self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
                self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                     self.util.filtered_matches_secondary,
                                                                                     self.kpi_name)
            self.calculate_brand_out_of_sub_category_sos()
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_brand_out_of_sub_category_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        filtered_matches = self.util.filtered_matches_secondary
        filtered_matches = filtered_matches[
            ~(filtered_matches[ScifConsts.SUB_CATEGORY_FK].isnull())]
        products_df = self.util.all_products[[MatchesConsts.PRODUCT_FK, ScifConsts.BRAND_FK, ScifConsts.CATEGORY_FK]]
        filtered_matches = filtered_matches.merge(products_df, on=MatchesConsts.PRODUCT_FK, how='left')
        sub_cat_df = filtered_matches.groupby([ScifConsts.SUB_CATEGORY_FK],
                                           as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
        sub_cat_df.rename(columns={MatchesConsts.WIDTH_MM_ADVANCE: 'sub_cat_len'}, inplace=True)
        brand_sub_cat_df = filtered_matches.groupby([ScifConsts.BRAND_FK, ScifConsts.SUB_CATEGORY_FK],
                                                 as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
        brand_sub_cat_df = brand_sub_cat_df.merge(sub_cat_df, on=ScifConsts.SUB_CATEGORY_FK, how='left')
        brand_sub_cat_df['sos'] = brand_sub_cat_df[MatchesConsts.WIDTH_MM_ADVANCE] / brand_sub_cat_df['sub_cat_len']
        for i, row in brand_sub_cat_df.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.BRAND_FK],
                                    numerator_result=row[MatchesConsts.WIDTH_MM_ADVANCE],
                                    denominator_id=row[ScifConsts.SUB_CATEGORY_FK],
                                    denominator_result=row['sub_cat_len'], result=row['sos'] * 100)