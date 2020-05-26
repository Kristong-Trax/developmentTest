from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class OOSMustHaveSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OOSMustHaveSKUKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.OOS_MUST_HAVE_SKU_KPI)
        assortment_df = self.utils.lvl3_assortment[self.utils.lvl3_assortment['kpi_fk_lvl2'] == kpi_fk]
        # todo: handle replacment skus
        for i, sku_row in assortment_df.iterrows():
            product_fk = sku_row['product_fk']
            result = sku_row['in_store']
            assortment_fk = sku_row['assortment_fk']
            score = 1 if result == 1 else 0
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=assortment_fk, score=score)

    def kpi_type(self):
        pass
