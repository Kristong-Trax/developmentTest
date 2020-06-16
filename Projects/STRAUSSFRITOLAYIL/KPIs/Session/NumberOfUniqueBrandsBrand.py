from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log


class NumberOfUniqueBrandsBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueBrandsBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_BRANDS_BRAND_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_UNQIUE_BRANDS_KPI]
        if len(template) == 1:
            categories = template.iloc[0][Consts.CATEGORY].split(",")
        elif len(template) != 1:
            Log.warning("There is more than one fitting row for KPI {}".format(str(kpi_fk)))
            categories = ['Crackers']
        else:
            categories = ['Crackers']
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['product_type'].isin([
            'SKU', 'Empty', 'Other'])]
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['category'].isin(categories)]
        own_manufacturer_matches['facings'] = 1
        # strauss are looking at sub_brand as brand in this KPI
        sub_brands = set(own_manufacturer_matches['sub_brand_fk'])
        for sub_brand_fk in sub_brands:
            sub_brand_df = own_manufacturer_matches[own_manufacturer_matches['sub_brand_fk'] == sub_brand_fk]
            result = score = sub_brand_df['facings'].sum()
            self.write_to_db_result(fk=kpi_fk, numerator_id=sub_brand_fk, result=result,
                                    denominator_id=self.utils.own_manuf_fk, score=score)

    def kpi_type(self):
        pass
