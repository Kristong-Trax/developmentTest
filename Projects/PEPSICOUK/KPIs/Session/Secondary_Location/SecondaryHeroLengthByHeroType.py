from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from KPIUtils_v2.Utils.Consts.DB import StaticKpis, SessionResultsConsts
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd


class SecondaryHeroLengthByHeroTypeKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryHeroLengthByHeroTypeKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):

        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        total_skus_in_ass = len(self.util.lvl3_ass_result)
        if not total_skus_in_ass:
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
            return
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        lvl3_ass_res_df = self.dependencies_data
        if lvl3_ass_res_df.empty and total_skus_in_ass:
            self.write_to_db_result(fk=kpi_fk, numerator_id=self.util.own_manuf_fk,
                                    result=0, score=0, denominator_id=self.util.store_id)
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
            return
        if not lvl3_ass_res_df.empty:
            available_hero_list = lvl3_ass_res_df[(lvl3_ass_res_df['numerator_result'] == 1)] \
                ['numerator_id'].unique().tolist()
            filtered_scif = self.util.filtered_scif_secondary[self.util.filtered_scif_secondary[ScifConsts.PRODUCT_FK].\
                isin(available_hero_list)]
            result_df = filtered_scif.groupby([self.util.HERO_SKU_LABEL], as_index=False).agg({'updated_gross_len':
                                                                                                   np.sum})
            result_df = result_df.merge(self.util.hero_type_custom_entity_df, left_on=self.util.HERO_SKU_LABEL,
                                        right_on='name', how='left')
            for i, row in result_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row['entity_fk'],
                                        denominator_id=row['entity_fk'], result=row['updated_gross_len'],
                                        score=row['updated_gross_len'])

        # add a function that resets to secondary scif and matches
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass
