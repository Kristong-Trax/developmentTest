from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log


class LSOSOwnBrandOutOfCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LSOSOwnBrandOutOfCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.LSOS_OWN_BRAND_OUT_OF_CATEGORY_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.LSOS_MANUFACTURER_OUT_OF_CATEGORY_KPI]
        if template.empty:
            template_categories = ['Crackers', 'Core Salty']
            target = -1
        # elif len(template) != 1:
        #     Log.warning("There is more than one fitting row for KPI {}".format(str(kpi_fk)))
        #     categories = ['Crackers', 'Core Salty']
        #     target = -1
        else:
            template_categories = set(template[Consts.CATEGORY])
        # template_category_fks = set(self.utils.all_products[self.utils.all_products[
        #     'category'].isin(template_categories)]['category_fk'])
        target_range = 5
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['stacking_layer'] == 1]
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches[
            'category'].isin(template_categories)]
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches[
            'product_type'].isin(['Empty', 'Other', 'SKU'])]
        # categories = set(own_manufacturer_matches['category_fk'])
        for category in template_categories:
            target = template[template['category'] == category][Consts.TARGET]
            if target:
                target = target.values[0]
            else:
                target = -1
            category_fk = self.utils.all_products[self.utils.all_products['category'] == category][
                'category_fk'].values[0]
            category_df = own_manufacturer_matches[own_manufacturer_matches['category'] == category]
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

