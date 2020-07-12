from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilitySkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilitySkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.HERO_SKU_AVAILABILITY_SKU)
        self.calculate_kpi_for_main_shelf()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass

    def calculate_kpi_for_main_shelf(self):
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        lvl3_ass_res = self.util.lvl3_ass_result
        if lvl3_ass_res.empty:
            return

        if not self.util.filtered_scif.empty:
            products_in_session = self.util.filtered_scif.loc[self.util.filtered_scif['facings'] > 0]['product_fk'].values
            products_df = self.util.all_products[[ScifConsts.PRODUCT_FK, ScifConsts.MANUFACTURER_FK]]
            lvl3_ass_res.loc[lvl3_ass_res['product_fk'].isin(products_in_session), 'in_store'] = 1
            lvl3_ass_res = lvl3_ass_res.merge(products_df, on=ScifConsts.PRODUCT_FK, how='left')
            for i, result in lvl3_ass_res.iterrows():
                score = result.in_store * 100
                custom_res = self.util.commontools.get_yes_no_result(score)
                self.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                        numerator_result=result.in_store, result=custom_res,
                                        denominator_id=result.manufacturer_fk, denominator_result=1, score=score,
                                        context_id=location_type_fk)
                self.util.add_kpi_result_to_kpi_results_df(
                    [result['kpi_fk_lvl3'], result['product_fk'], self.util.store_id, custom_res,
                     score, None])
