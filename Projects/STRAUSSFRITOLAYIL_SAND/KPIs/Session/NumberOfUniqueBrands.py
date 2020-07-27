from Projects.STRAUSSFRITOLAYIL_SAND.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL_SAND.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log


class NumberOfUniqueBrandsKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueBrandsKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_BRANDS_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_UNQIUE_BRANDS_KPI]
        sku_results = self.dependencies_data
        if len(template) == 1:
            target = template.iloc[0][Consts.TARGET_MAX]
        else:
            Log.warning("There are no fitting rows or more than one fitting row for KPI {}".format(str(kpi_fk)))
            target = None
        # strauss are looking at sub_brand as brand in this KPI
        if sku_results.empty:
            number_of_sub_brands = 0
        else:
            number_of_sub_brands = len(sku_results[sku_results['result'] >= 1])
        if not target:
            score = Consts.NO_TARGET
        else:
            score = Consts.PASS if number_of_sub_brands >= target else Consts.FAIL
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk, target=target,
                                result=number_of_sub_brands, denominator_id=self.utils.store_id, score=score)

    def kpi_type(self):
        pass

