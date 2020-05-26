from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class SKULinearKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SKULinearKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.SKU_LINEAR_KPI)
        custom_scif = self.utils.scif.copy()
        custom_scif = custom_scif[custom_scif['template_name'] == 'Primary Shelf']
        custom_scif = custom_scif[~custom_scif['product_type'].isin(['Irrelevant'])]
        custom_scif = custom_scif[['product_fk', 'manufacturer_fk', 'gross_len_ign_stack']]
        combined_scenes_scif = custom_scif.groupby(['product_fk', 'manufacturer_fk']).sum()
        for i, sku_row in combined_scenes_scif.iterrows():
            product_fk = sku_row['product_fk']
            result = sku_row['gross_len_ign_stack']
            manufacturer_fk = sku_row['manufacturer_fk']
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=manufacturer_fk, score=result)

    def kpi_type(self):
        pass
