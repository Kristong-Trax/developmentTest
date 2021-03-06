from Projects.STRAUSSFRITOLAYIL_SAND.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL_SAND.Data.LocalConsts import Consts


class OOSMustHaveKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OOSMustHaveKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.OOS_MUST_HAVE_KPI)
        sku_results = self.dependencies_data
        if sku_results.empty:
            return
        assortment_fks = set(sku_results['denominator_id'])
        for assortment_fk in assortment_fks:
            assortment_df = sku_results[sku_results['denominator_id'] == assortment_fk]
            denominator = len(assortment_df)
            numerator = len(assortment_df[assortment_df['result'] == Consts.OOS])
            result = self.utils.calculate_sos_result(numerator, denominator)
            self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk,
                                    denominator_id=self.utils.store_id, context_id=assortment_fk, result=result,
                                    numerator_result=numerator, denominator_result=denominator)

    def kpi_type(self):
        pass
