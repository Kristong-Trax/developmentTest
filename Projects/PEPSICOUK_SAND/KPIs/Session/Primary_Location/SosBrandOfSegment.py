from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts
import pandas as pd
import numpy as np


class SosBrandOfSegmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosBrandOfSegmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.BRAND_SOS_OF_SEGMENT)
        self.calculate_brand_out_of_sub_category_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_brand_out_of_sub_category_sos(self):
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.BRAND_SOS_OF_SEGMENT)
        filtered_matches = self.util.filtered_matches
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
                                    denominator_result=row['sub_cat_len'], result=row['sos'] * 100,
                                    context_id=location_type_fk)
            self.util.add_kpi_result_to_kpi_results_df(
                        [kpi_fk, row[ScifConsts.BRAND_FK], row[ScifConsts.SUB_CATEGORY_FK], row['sos'] * 100, None,
                         None])