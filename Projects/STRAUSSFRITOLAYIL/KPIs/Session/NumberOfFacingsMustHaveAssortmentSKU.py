from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class NumberOfFacingsMustHaveAssortmentSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfFacingsMustHaveAssortmentSKUKpi, self).__init__(data_provider, config_params=config_params,
                                                                      **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        return
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_FACINGS_MUST_HAVE_KPI)
        own_manufacturer_scif = self.utils.scif[self.utils.scif['manufacturer_fk'] == self.utils.own_manuf_fk]
        own_manufacturer_scif = own_manufacturer_scif[~own_manufacturer_scif['product_type'].isin(['Other', 'Empty'])]
        for i, sku_row in own_manufacturer_scif.iterrows():
            product_fk = sku_row['product_fk']
            result = sku_row['facings']
            score = 1 if result > 0 else 0
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=self.utils.own_manuf_fk, score=score)

    def kpi_type(self):
        pass