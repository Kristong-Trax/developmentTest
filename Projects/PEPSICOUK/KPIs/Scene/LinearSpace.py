from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class LinearSpaceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LinearSpaceKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.TOTAL_LINEAR_SPACE)
            for i, row in self.util.filtered_scif.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'],
                                                    denominator_id=self.util.store_id,
                                                    result=row['gross_len_add_stack'], by_scene=True)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, row['product_fk'], self.util.store_id, row['gross_len_add_stack'], None])