from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
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
        if template.empty:
            target = -1
        elif len(template) != 1:
            Log.warning("There is more than one fitting row for KPI {}".format(str(kpi_fk)))
            target = -1
        else:
            target = template['Target'][0]
        # strauss are looking at sub_brand as brand in this KPI
        number_of_sub_brands = len(sku_results)
        score = 1 if number_of_sub_brands >= target else 0
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk,
                                result=number_of_sub_brands, denominator_id=self.utils.store_id, score=score)

    def kpi_type(self):
        pass

