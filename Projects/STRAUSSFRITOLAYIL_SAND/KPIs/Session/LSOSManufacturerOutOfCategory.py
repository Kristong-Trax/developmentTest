from Projects.STRAUSSFRITOLAYIL_SAND.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL_SAND.Data.LocalConsts import Consts


class LSOSManufacturerOutOfCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LSOSManufacturerOutOfCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.LSOS_MANUFACTURER_OUT_OF_CATEGORY_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.LSOS_MANUFACTURER_OUT_OF_CATEGORY_KPI]
        if template.empty:
            template_categories = ['Crackers', 'Core Salty']
        else:
            template_categories = set(template[Consts.CATEGORY])
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['stacking_layer'] == 1]
        all_store_matches = self.utils.match_product_in_scene_wo_hangers.copy()
        all_store_matches = all_store_matches[all_store_matches['stacking_layer'] == 1]
        for category in template_categories:
            target = template[template[Consts.CATEGORY] == category][Consts.TARGET]
            if not target.empty:
                target = target.values[0] * 100
            else:
                target = None
            category_fk = self.utils.all_products[self.utils.all_products['category'] == category][
                'category_fk'].values[0]
            own_skus_category_df = own_manufacturer_matches[own_manufacturer_matches['category_fk'] == category_fk]
            store_category_df = all_store_matches[all_store_matches['category_fk'] == category_fk]
            own_category_linear_length = own_skus_category_df['width_mm_advance'].sum()
            store_category_linear_length = store_category_df['width_mm_advance'].sum()
            sos_result = self.utils.calculate_sos_result(own_category_linear_length, store_category_linear_length)
            if not target:
                kpi_score = Consts.NO_TARGET
            else:
                kpi_score = Consts.PASS if (target <= sos_result) else Consts.FAIL
            self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk, denominator_id=category_fk,
                                    numerator_result=own_category_linear_length, target=target,
                                    denominator_result=store_category_linear_length,
                                    result=sos_result, score=kpi_score)

    def kpi_type(self):
        pass

