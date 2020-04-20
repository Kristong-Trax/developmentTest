from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetHeroSkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetHeroSkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.HERO_SKU_SOS)
        # self.calculate_hero_sku_sos_vs_target(sos_targets)
        self.calculate_hero_sku_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_hero_sku_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_SOS)
        filtered_scif = self.util.filtered_scif
        # category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
        #                                     as_index=False).agg({'updated_gross_length': np.sum})
        # category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
        # av_hero_list = self.util.get_available_hero_sku_list(self.dependencies_data)

        # filtered_scif = filtered_scif[filtered_scif[ScifConsts.PRODUCT_FK].isin(av_hero_list)]
        if (not filtered_scif.empty) and (not self.dependencies_data.empty):
            category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                                as_index=False).agg({'updated_gross_length': np.sum})
            category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)

            av_hero_list = self.util.get_available_hero_sku_list(self.dependencies_data)
            filtered_scif = filtered_scif[filtered_scif[ScifConsts.PRODUCT_FK].isin(av_hero_list)]

            unav_hero_list = self.util.get_unavailable_hero_sku_list(self.dependencies_data)
            unav_hero_df = self.util.all_products[self.util.all_products[ScifConsts.PRODUCT_FK].isin(unav_hero_list)] \
                [[ScifConsts.PRODUCT_FK, ScifConsts.CATEGORY_FK]]
            unav_hero_df['updated_gross_length'] = 0
            filtered_scif = filtered_scif.append(unav_hero_df)

            hero_cat_df = filtered_scif.groupby([ScifConsts.PRODUCT_FK, ScifConsts.CATEGORY_FK],
                                                as_index=False).agg({'updated_gross_length': np.sum})
            hero_cat_df = hero_cat_df.merge(category_df, on=ScifConsts.CATEGORY_FK, how='left')
            hero_cat_df['cat_len'] = hero_cat_df['cat_len'].fillna(0)
            hero_cat_df['sos'] = hero_cat_df.apply(self.calculate_sos, axis=1)
            for i, row in hero_cat_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.PRODUCT_FK],
                                        numerator_result=row['updated_gross_length'],
                                        denominator_id=row[ScifConsts.CATEGORY_FK],
                                        denominator_result=row['cat_len'], result=row['sos'])
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, row[ScifConsts.PRODUCT_FK], row[ScifConsts.CATEGORY_FK], row['sos'], None, None])

    @staticmethod
    def calculate_sos(row):
        sos = 0
        if row['cat_len'] != 0:
            sos = float(row['updated_gross_length']) / row['cat_len'] * 100
        return sos