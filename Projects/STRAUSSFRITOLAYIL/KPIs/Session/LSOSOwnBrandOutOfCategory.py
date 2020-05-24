from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class LSOSOwnBrandOutOfCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LSOSOwnBrandOutOfCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        return
        sku_results = self.dependencies_data
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.SOS_BRAND_OUT_OF_CATEGORY_KPI)
        result = len(sku_results)
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk,
                                result=result, denominator_id=self.utils.store_id)
        # self.util.add_kpi_result_to_kpi_results_df(
        #     [distribution_kpi_fk, self.util.own_manuf_fk, self.util.store_id, res, score])

    def kpi_type(self):
        pass

