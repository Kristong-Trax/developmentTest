from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoConsts


class RelativePostion(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(RelativePostion, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):

        return DiageoKpiNames.RELATIVE_POSITION

    def calculate(self):
        """
        this class calculates all the menu KPI's depending on the parameter given
        """
        # aggregation level
        res_list = []
        depend = self.dependencies_data
        scores_sum = depend[SessionResultsConsts.SCORE].sum()
        scores_len = len(depend[SessionResultsConsts.SCORE].index)

        parent_result = scores_sum / float(scores_len) * 100
        numerator_id = self.util.own_manuf_fk
        res_list.append(self.util.build_dictionary_for_db_insert_v2(kpi_name=DiageoConsts.RELATIVE_POSITION,
                                                                    numerator_id=numerator_id,
                                                                    denominator_id=self.util.store_id,
                                                                    numerator_result=scores_sum,
                                                                    denominator_result=scores_len,
                                                                    result=parent_result, score=parent_result,
                                                                    identifier_result=DiageoConsts.RELATIVE_POSITION))
        for res in res_list:
            self.write_to_db_result(**res)
