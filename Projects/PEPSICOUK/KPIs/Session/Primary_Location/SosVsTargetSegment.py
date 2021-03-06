from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts
import numpy as np


class SosVsTargetSegmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetSegmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.PEPSICO_SEGMENT_SOS)
        # self.calculate_pepsico_segment_space_sos_vs_target(sos_targets)
        self.calculate_pepsico_segment_space_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_pepsico_segment_space_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PEPSICO_SEGMENT_SOS)
        filtered_matches = self.util.filtered_matches
        products_df = self.util.all_products[[MatchesConsts.PRODUCT_FK, ScifConsts.BRAND_FK, ScifConsts.CATEGORY_FK]]
        filtered_matches = filtered_matches.merge(products_df, on=MatchesConsts.PRODUCT_FK, how='left')
        cat_df = filtered_matches.groupby([ScifConsts.CATEGORY_FK],
                                          as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
        cat_df.rename(columns={MatchesConsts.WIDTH_MM_ADVANCE: 'cat_len'}, inplace=True)
        # filtered_scif = filtered_scif[filtered_scif[ScifConsts.MANUFACTURER_FK] == self.util.own_manuf_fk]
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        if not filtered_matches.empty:
            sub_cat_df = filtered_matches.groupby([ScifConsts.SUB_CATEGORY_FK, ScifConsts.CATEGORY_FK],
                                               as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
            if not sub_cat_df.empty:
                sub_cat_df = sub_cat_df.merge(cat_df, on=ScifConsts.CATEGORY_FK, how='left')
                sub_cat_df['sos'] = sub_cat_df[MatchesConsts.WIDTH_MM_ADVANCE] / sub_cat_df['cat_len']
                for i, row in sub_cat_df.iterrows():
                    self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.SUB_CATEGORY_FK],
                                            numerator_result=row[MatchesConsts.WIDTH_MM_ADVANCE],
                                            denominator_id=row[ScifConsts.CATEGORY_FK],
                                            denominator_result=row['cat_len'], result=row['sos'] * 100,
                                            context_id=location_type_fk)
                    self.util.add_kpi_result_to_kpi_results_df(
                        [kpi_fk, row[ScifConsts.SUB_CATEGORY_FK], row[ScifConsts.CATEGORY_FK], row['sos'] * 100,
                         None, None])

