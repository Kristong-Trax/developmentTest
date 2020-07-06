from Projects.STRAUSSFRITOLAYIL_SAND.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL_SAND.Data.LocalConsts import Consts


class OOSNpaSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OOSNpaSKUKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.OOS_NPA_SKU_KPI)
        assortment_df = self.utils.lvl3_assortment[self.utils.lvl3_assortment['kpi_fk_lvl3'] == kpi_fk]
        for i, sku_row in assortment_df.iterrows():
            product_fk = sku_row['product_fk']
            in_store = sku_row['in_store']
            facings = sku_row['facings_all_products']
            assortment_fk = sku_row['assortment_fk']
            result = Consts.DISTRIBUTED if (in_store == 1) else Consts.OOS
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=assortment_fk, score=facings)

    def kpi_type(self):
        pass
