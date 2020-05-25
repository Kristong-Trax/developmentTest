from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class NumberOfUniqueBrandsBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueBrandsBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_BRANDS_BRAND_KPI)
        own_manufacturer_skus = self.utils.scif[self.utils.scif['manufacturer_fk'] == self.utils.own_manuf_fk]
        own_manufacturer_skus = own_manufacturer_skus[~own_manufacturer_skus['product_type'].isin(['Empty'])]
        # strauss are looking at sub_brand as brand in this KPI
        sub_brands = set(own_manufacturer_skus['sub_brand_fk'])
        for sub_brand_fk in sub_brands:
            sub_brand_df = own_manufacturer_skus[own_manufacturer_skus['sub_brand_fk'] == sub_brand_fk]
            result = score = sub_brand_df['facings'].sum()
            self.write_to_db_result(fk=kpi_fk, numerator_id=sub_brand_fk, result=result,
                                    denominator_id=self.utils.own_manuf_fk, score=score)

    def kpi_type(self):
        pass
