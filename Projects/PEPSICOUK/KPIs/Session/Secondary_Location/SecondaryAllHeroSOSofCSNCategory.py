from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from Trax.Data.ProfessionalServices.PsConsts.DB import StaticKpis, SessionResultsConsts
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd


class SecondaryAllHeroSOSofCSNCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryAllHeroSOSofCSNCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
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
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
            return
        lvl3_ass_res_df = self.dependencies_data
        if lvl3_ass_res_df.empty:
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
            return
        # talk to Nitzan
        if not lvl3_ass_res_df.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
            filtered_scif = self.util.filtered_scif_secondary
            category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                                as_index=False).agg({'updated_gross_length': np.sum})
            category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)

            available_hero_list = lvl3_ass_res_df[(lvl3_ass_res_df['numerator_result'] == 1)] \
                ['numerator_id'].unique().tolist()
            filtered_scif = filtered_scif[filtered_scif[ScifConsts.PRODUCT_FK].isin(available_hero_list)]

            all_hero_cat_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK], as_index=False).\
                agg({'updated_gross_length': np.sum})
            all_hero_cat_df = all_hero_cat_df.merge(category_df, on=ScifConsts.CATEGORY_FK, how='left')
            all_hero_cat_df['cat_len'] = all_hero_cat_df['cat_len'].fillna(0)
            if not all_hero_cat_df.empty:
                all_hero_cat_df['sos'] = all_hero_cat_df.apply(self.calculate_sos, axis=1)
                for i, row in all_hero_cat_df.iterrows():
                    self.write_to_db_result(fk=kpi_fk, numerator_id=self.util.own_manuf_fk,
                                            numerator_result=row['updated_gross_length'],
                                            denominator_id=row[ScifConsts.CATEGORY_FK],
                                            denominator_result=row['cat_len'], result=row['sos'])

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
