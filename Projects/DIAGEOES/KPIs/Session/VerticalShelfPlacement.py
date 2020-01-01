from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts


class DiageoShelfPlacement(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(DiageoShelfPlacement, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.VERTICAL_SHELF_PLACEMENT

    def calculate(self):
        """
        this function gets the data from the SKU KPI and calcs the KPU at store level
        :return:
        """
        res_list = []
        SKU_level_results = self.dependencies_data  # get SKU level data
        total = len(SKU_level_results.index)  # count it
        Passed = SKU_level_results[SKU_level_results[SessionResultsConsts.RESULT] > 0]
        correctly_positioned_total = len(Passed.index) * 100  # count and calc all store score
        vsp_kpi_fk = self.util.commonV2.get_kpi_fk_by_kpi_type(DiageoKpiNames.VERTICAL_SHELF_PLACEMENT)
        total_score = correctly_positioned_total / float(total) if total else 0
        own_manufacturer = self.data_provider.own_manufacturer.param_value.iloc[0]
        res_list.append(self.util.build_dictionary_for_db_insert_v2(fk=vsp_kpi_fk, numerator_id=own_manufacturer,
                                                               numerator_result=correctly_positioned_total / 100.0,
                                                               score=total_score, denominator_id=self.util.store_id,
                                                               denominator_result=total, result=total_score,
                                                               identifier_result=vsp_kpi_fk))
        for res in res_list:
            self.write_to_db_result(**res)
