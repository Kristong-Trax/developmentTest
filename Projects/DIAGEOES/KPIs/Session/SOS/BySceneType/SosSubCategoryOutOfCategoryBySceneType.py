from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SceneResultsConsts
from Projects.DIAGEORU.KPIs.Session.SOS.BySceneType.SosSceneTypeClasses import \
    SubCategoryFacingsSOSPerCategoryInSceneType


class DiageoSosOutOfStoreBySceneType(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(DiageoSosOutOfStoreBySceneType, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return'SOS SUB CATEGORY OUT OF CATEGORY BY SCENE TYPE'

    def calculate(self):
        """
        uses the OOTB class to calc by scene type
        """
        # level 3 in hierarchy
        res_list = []
        sos_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('SOS SUB CATEGORY OUT OF CATEGORY BY SCENE TYPE')
        sos_sub_cat_out_of_cat_by_scene_type = SubCategoryFacingsSOSPerCategoryInSceneType(data_provider=self.data_provider,
                                                                                   kpi_definition_fk=sos_fk).calculate()
        for row in sos_sub_cat_out_of_cat_by_scene_type:
            res_list.append(self.util.build_dictionary_for_db_insert_v2(fk=sos_fk, numerator_id=row.numerator_id,
                                                                        numerator_result=row.numerator_result,
                                                                        denominator_id=row.denominator_id,
                                                                        denominator_result=row.denominator_result,
                                                                        target=row.target,
                                                                        result=row.result,
                                                                        score=row.score,
                                                                        context_id=row.context_id))
        for res in res_list:
            self.write_to_db_result(**res)