from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class HeroPlacementKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroPlacementKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        child_kpi_results = self.dependencies_data
        if not child_kpi_results.empty:
            top_sku_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_PLACEMENT)
            res = len(child_kpi_results)
            self.write_to_db_result(fk=top_sku_parent, numerator_id=self.util.own_manuf_fk,
                                    denominator_id=self.util.store_id, result=res, score=res)
            self.util.add_kpi_result_to_kpi_results_df(
                [top_sku_parent, self.util.own_manuf_fk, self.util.store_id, res, res])
