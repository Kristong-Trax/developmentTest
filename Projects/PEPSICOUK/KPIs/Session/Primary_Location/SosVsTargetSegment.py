from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
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
        filtered_scif = self.util.filtered_scif
        cat_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                       as_index=False).agg({'updated_gross_length': np.sum})
        cat_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
        filtered_scif = filtered_scif[filtered_scif[ScifConsts.MANUFACTURER_FK] == self.util.own_manuf_fk]
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        if not filtered_scif.empty:
            man_cat_df = filtered_scif.groupby([ScifConsts.MANUFACTURER_FK, ScifConsts.CATEGORY_FK],
                                                as_index=False).agg({'updated_gross_length': np.sum})
            if not man_cat_df.empty:
                man_cat_df = man_cat_df.merge(cat_df, on=ScifConsts.CATEGORY_FK, how='left')
                man_cat_df['sos'] = man_cat_df['updated_gross_length'] / man_cat_df['cat_len']
                for i, row in man_cat_df.iterrows():
                    self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.MANUFACTURER_FK],
                                            numerator_result=row['updated_gross_length'],
                                            denominator_id=row[ScifConsts.CATEGORY_FK],
                                            denominator_result=row['cat_len'], result=row['sos'] * 100,
                                            context_id=location_type_fk)
                    self.util.add_kpi_result_to_kpi_results_df(
                        [kpi_fk, row[ScifConsts.MANUFACTURER_FK], row[ScifConsts.CATEGORY_FK], row['sos'] * 100,
                         None, None])

