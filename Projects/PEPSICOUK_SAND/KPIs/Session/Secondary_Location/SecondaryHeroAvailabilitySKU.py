from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilitySkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilitySkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        self.calculate_kpi_for_secondary_shelf()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass

    def calculate_kpi_for_secondary_shelf(self):
        # scif for secondary should have display and store_area breakdown

        lvl3_ass_res = self.util.lvl3_ass_result
        if lvl3_ass_res.empty:
            return
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        ass_list = lvl3_ass_res[ScifConsts.PRODUCT_FK].values.tolist()
        filtered_scif = self.util.filtered_scif_secondary[self.util.filtered_scif_secondary[ScifConsts.PRODUCT_FK]. \
            isin(ass_list)]
        # products_in_session = filtered_scif.loc[filtered_scif['facings'] > 0]['product_fk'].values
        # lvl3_ass_res.loc[lvl3_ass_res['product_fk'].isin(products_in_session), 'in_store'] = 1
        assortment_scif = filtered_scif.drop_duplicates(subset=[ScifConsts.TEMPLATE_FK, 'store_area_fk',
                                                                ScifConsts.PRODUCT_FK])
        for i, result in assortment_scif.iterrows():
            score = 100
            custom_res = self.util.commontools.get_yes_no_result(score)
            self.write_to_db_result(fk=kpi_fk, numerator_id=result.product_fk,
                                    numerator_result=1, result=custom_res,
                                    denominator_id=result.template_fk, denominator_result=1, score=score,
                                    context_id=result.store_area_fk)
