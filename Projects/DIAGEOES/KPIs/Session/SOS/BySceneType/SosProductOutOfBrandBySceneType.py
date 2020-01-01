from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SceneResultsConsts
from Projects.DIAGEORU.KPIs.Session.SOS.BySceneType.SosSceneTypeClasses import \
    ManufacturerFacingsSOSProductPerBrandInSceneType


class SosProductOutOfBrandBySceneType(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosProductOutOfBrandBySceneType, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return'SOS BRAND OUT OF SUB CATEGORY BY SCENE TYPE'

    def calculate(self):
        """
        uses the OOTB class to calc by scene type
        """
        #  level 6 in the hierarchy
        res_list = []
        sos_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('SOS BRAND OUT OF SUB CATEGORY BY SCENE TYPE')
        sos_product_out_of_brand_by_scene_type = ManufacturerFacingsSOSProductPerBrandInSceneType(
            data_provider=self.data_provider, kpi_definition_fk=sos_fk).calculate()
        for row in sos_product_out_of_brand_by_scene_type:
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