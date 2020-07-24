from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from Trax.Data.ProfessionalServices.PsConsts.DB import StaticKpis, SessionResultsConsts
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd


class SecondaryHeroSOSofCategorySKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryHeroSOSofCategorySKUKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        # pass secondary scif and matches
        if self.util.commontools.are_all_bins_tagged:
            self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
                self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                     self.util.filtered_matches_secondary,
                                                                                     self.kpi_name)
            total_skus_in_ass = len(self.util.lvl3_ass_result)
            if not total_skus_in_ass:
                self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
                return
            lvl3_ass_res_df = self.dependencies_data
            if lvl3_ass_res_df.empty:
                self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
                return

            if not lvl3_ass_res_df.empty:
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
                filtered_scif = self.util.filtered_scif_secondary \
                    [self.util.filtered_scif_secondary[ScifConsts.CATEGORY]=='CSN']
                display_location_df = filtered_scif.groupby([ScifConsts.TEMPLATE_FK, 'store_area_fk'],
                                                            as_index=False).agg({'updated_gross_length': np.sum})

                display_location_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
                available_hero_list = lvl3_ass_res_df[(lvl3_ass_res_df['numerator_result'] == 1)]\
                    ['numerator_id'].unique().tolist()
                filtered_scif = filtered_scif[filtered_scif[ScifConsts.PRODUCT_FK].isin(available_hero_list)]

                hero_display_location_df = filtered_scif.groupby([ScifConsts.PRODUCT_FK, ScifConsts.TEMPLATE_FK,
                                                                  'store_area_fk'], as_index=False). \
                    agg({'updated_gross_length': np.sum})
                hero_display_location_df = hero_display_location_df.merge(display_location_df,
                                                                          on=[ScifConsts.TEMPLATE_FK, 'store_area_fk'],
                                                                          how='left')
                hero_display_location_df['cat_len'] = hero_display_location_df['cat_len'].fillna(0)
                if not hero_display_location_df.empty:
                    hero_display_location_df['sos'] = hero_display_location_df.apply(self.calculate_sos, axis=1)
                    for i, row in hero_display_location_df.iterrows():
                        self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.PRODUCT_FK],
                                                numerator_result=row['updated_gross_length'],
                                                denominator_id=row[ScifConsts.TEMPLATE_FK],
                                                denominator_result=row['cat_len'], result=row['sos'],
                                                context_id=row['store_area_fk'])

            # add a function that resets to secondary scif and matches
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass

    @staticmethod
    def calculate_sos(row):
        sos = 0
        if row['cat_len'] != 0:
            sos = float(row['updated_gross_length']) / row['cat_len'] * 100
        return sos
