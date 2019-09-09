from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoSecondaryDisplays(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoSecondaryDisplays, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.SECONDARY_DISPLAYS

    def calculate(self):
        res_json = self.util.diageo_generator.diageo_global_secondary_display_secondary_function()
        if res_json:
            self.write_to_db_result(fk=res_json['fk'], numerator_id=1, denominator_id=self.util.store_id,
                                    result=res_json[SessionResultsConsts.RESULT])

