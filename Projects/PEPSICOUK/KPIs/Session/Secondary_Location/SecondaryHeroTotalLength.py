from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from KPIUtils_v2.Utils.Consts.DB import StaticKpis, SessionResultsConsts
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd


class SecondaryHeroTotalLengthKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryHeroTotalLengthKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        # pass secondary scif and matches
        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        total_skus_in_ass = len(self.util.lvl3_ass_result)
        if not total_skus_in_ass:
            return
        lvl3_ass_res_df = self.dependencies_data
        if lvl3_ass_res_df.empty:
            self.write_to_db_result(fk=self.kpi_name, numerator_id=self.util.own_manuf_fk,
                                    result=0, score=0, denominator_id=self.util.store_id)
            return
        if not lvl3_ass_res_df.empty:
            available_hero_list = self.util.get_available_hero_sku_list(lvl3_ass_res_df)
            hero_len = self.util.filtered_scif[self.util.filtered_scif[ScifConsts.PRODUCT_FK].isin(available_hero_list)]\
                        ['updated_gross_len'].sum()
            self.write_to_db_result(fk=self.kpi_name, numerator_id=self.util.own_manuf_fk,
                                    denominator_id=self.util.store_id, result=hero_len, score=hero_len)

        # add a function that resets to secondary scif and matches
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass
