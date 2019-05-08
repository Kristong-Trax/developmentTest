from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class NumberOfBaysKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfBaysKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            bays_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.NUMBER_OF_BAYS)
            matches = self.util.match_product_in_scene[~(self.util.match_product_in_scene['bay_number'] == -1)]
            bays_in_scene = matches['bay_number'].unique().tolist()
            bays_num = len(bays_in_scene)
            self.util.common.write_to_db_result(fk=bays_kpi_fk, numerator_id=self.util.own_manuf_fk, result=bays_num,
                                                denominator_id=self.util.store_id, by_scene=True)
            self.util.add_kpi_result_to_kpi_results_df([bays_kpi_fk, self.util.own_manuf_fk,
                                                        self.util.store_id, bays_num, None])