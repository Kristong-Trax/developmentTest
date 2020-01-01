from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts

class BrandBlock(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BrandBlock, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):

        return DiageoKpiNames.BRAND_BLOCKING

    def calculate(self):
        """
        this class calculates all the menu KPI's depending on the parameter given
        """
        kpi_name = DiageoKpiNames.BRAND_BLOCKING
        template_data = self.util.template_handler.download_template(DiageoKpiNames.BRAND_BLOCKING)
        depend = self.dependencies_data
        res_list = []
        scores_sum = depend[SessionResultsConsts.SCORE].sum()
        scores_len = len(depend[SessionResultsConsts.SCORE].index)
        numerator_id = self.util.own_manuf_fk
        if numerator_id not in set(self.util.scif.manufacturer_fk):
            self.util.Log.debug('No products for own Manufacturer')
        else:
            set_score = (scores_sum / float(scores_len)) * 100
            res_list.append(self.util.build_dictionary_for_db_insert_v2(
                kpi_name=kpi_name, result=set_score, score=set_score, numerator_id=numerator_id,
                numerator_result=scores_sum, denominator_result=scores_len, denominator_id=self.util.store_id,
                identifier_result=kpi_name))
        for res in res_list:
            self.write_to_db_result(**res)
