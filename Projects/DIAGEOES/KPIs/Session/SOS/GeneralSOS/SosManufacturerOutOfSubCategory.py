from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SceneResultsConsts


class SosManufacturerOutOfSubCategory(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosManufacturerOutOfSubCategory, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return'SOS MANUFACTURER OUT OF SUB CATEGORY'

    def calculate(self):
        """
        simply takes the OOTB kpi calculated and renames the results.
        """
        # level 4
        OOTB_Data = self.dependencies_data
        res_list = []
        sos_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('SOS MANUFACTURER OUT OF SUB CATEGORY')
        for index, row in OOTB_Data.head().iterrows():
            res_list.append(self.util.build_dictionary_for_db_insert_v2(fk=sos_fk, numerator_id=row[
                SceneResultsConsts.NUMERATOR_ID],
                                                                        numerator_result=row[
                                                                            SceneResultsConsts.NUMERATOR_RESULT],
                                                                        denominator_id=row[
                                                                            SceneResultsConsts.DENOMINATOR_ID],
                                                                        denominator_result=row[
                                                                            SceneResultsConsts.DENOMINATOR_RESULT],
                                                                        target=row[SceneResultsConsts.TARGET],
                                                                        result=row[SceneResultsConsts.RESULT],
                                                                        score=row[SceneResultsConsts.SCORE],
                                                                        context_id=row[SceneResultsConsts.CONTEXT_ID]))
        for res in res_list:
            self.write_to_db_result(**res)