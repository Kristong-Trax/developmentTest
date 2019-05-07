from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class AssortmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(AssortmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil()

    def calculate(self):
        oos_sku_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_OOS_SKU)
        oos_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_OOS)
        distribution_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY)
        identifier_parent = self.util.common.get_dictionary(kpi_fk=distribution_kpi_fk)
        oos_identifier_parent = self.util.common.get_dictionary(kpi_fk=oos_fk)
        for i, result in self.util.lvl3_ass_result.iterrows():
            score = result.in_store * 100
            custom_res = self.util.commontools.get_yes_no_result(score)
            self.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                           numerator_result=result.in_store, result=custom_res,
                                           denominator_id=self.util.store_id, should_enter=True,
                                           denominator_result=1, score=score, identifier_parent=identifier_parent)
            self.util.add_kpi_result_to_kpi_results_df(
                [result['kpi_fk_lvl3'], result['product_fk'], self.util.store_id, custom_res,
                 score])
            if score == 0:
                # OOS
                self.write_to_db_result(fk=oos_sku_fk,
                                               numerator_id=result.product_fk, numerator_result=score,
                                               result=score, denominator_id=self.util.store_id,
                                               denominator_result=1, score=score,
                                               identifier_parent=oos_identifier_parent, should_enter=True)

        if not self.util.lvl3_ass_result.empty:
            lvl2_result = self.util.assortment.calculate_lvl2_assortment(self.util.lvl3_ass_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if result.target and not np.math.isnan(result.target):
                    if result.group_target_date <= self.util.visit_date:
                        denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res)) * 100
                score = 100 if res >= 100 else 0
                self.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=self.util.own_manuf_fk,
                                               numerator_result=result.passes, result=res,
                                               denominator_id=self.util.store_id, denominator_result=denominator_res,
                                               score=score,
                                               identifier_result=identifier_parent, should_enter=True)
                self.util.add_kpi_result_to_kpi_results_df(
                    [result.kpi_fk_lvl2, self.util.own_manuf_fk, self.util.store_id, res, score])

                self.write_to_db_result(fk=oos_fk, numerator_id=self.util.own_manuf_fk,
                                               numerator_result=denominator_res - result.passes,
                                               denominator_id=self.util.store_id, denominator_result=denominator_res,
                                               result=100 - res, score=(100 - res),
                                               identifier_result=oos_identifier_parent,
                                               should_enter=True)

    def kpi_type(self):
        pass