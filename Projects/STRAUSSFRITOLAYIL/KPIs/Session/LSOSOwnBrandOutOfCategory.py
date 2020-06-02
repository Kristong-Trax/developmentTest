from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class LSOSOwnBrandOutOfCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LSOSOwnBrandOutOfCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.LSOS_OWN_BRAND_OUT_OF_CATEGORY_KPI)
        # todo: implement target
        # target = self.utils.kpi_external_targets['taregt']
        target = 30
        target_range = 5
        # todo: implement category extraction
        category_fks = [1, 2]
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['stacking_layer'] == 1]
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['category_fk'].isin(category_fks)]
        categories = set(own_manufacturer_matches['category_fk'])
        for category_fk in categories:
            category_df = own_manufacturer_matches[own_manufacturer_matches['category_fk'] == category_fk]
            category_linear_length = category_df['width_mm_advance'].sum()
            # strauss are looking at sub_brand as brand in this KPI
            sub_brands = set(category_df['sub_brand_fk'])
            for sub_brand_fk in sub_brands:
                sub_brand_df = category_df[category_df['sub_brand_fk'] == sub_brand_fk]
                sub_brand_linear_length = sub_brand_df['width_mm_advance'].sum()
                sos_result = self.utils.calculate_sos_result(sub_brand_linear_length, category_linear_length)
                kpi_score = 1 if ((target - target_range) <= sos_result <= (target + target_range)) else 0
                self.write_to_db_result(fk=kpi_fk, numerator_id=sub_brand_fk, denominator_id=category_fk,
                                        numerator_result=sub_brand_linear_length,
                                        denominator_result=category_linear_length, result=sos_result, score=kpi_score)

    def kpi_type(self):
        pass

