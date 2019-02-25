from Projects.GFKDE.PresenceOnGondolaEndBrandSubCategory import PresenceonGondolaEndBrandSubCategory_KPI
from Projects.GFKDE.Tests.Sessions_Data import data
from Trax.Algo.Calculations.OutOfTheBox.Calculations.Tests.Base import BaseCalculationTest


class TestWashingMachinesSOSBrandCategory(BaseCalculationTest):
    def import_path(self):
        return 'Projects.GFKDE.SOSUnboxedBrandCategory.PresenceonGondolaEndBrandSubCategory_KPI'

    def set_up(self):
        super(TestWashingMachinesSOSBrandCategory, self).set_up()
        self._kpi_definition_fk = 10

    def test_calculate_return_resuls(self):
        self._mock_scene_item_facts(data=data)
        calculation = PresenceonGondolaEndBrandSubCategory_KPI(self._data_provider_mock, self._kpi_definition_fk)
        kpi_type, result_df = calculation.calculate()
        brand_110_category_3 = result_df[(result_df["numerator_id"] == 110) & (result_df["denominator_id"]== 3)]
        brand_113_category_3 = result_df[(result_df["numerator_id"] == 113) & (result_df["denominator_id"] == 3)]
        brand_115_category_3 = result_df[(result_df["numerator_id"] == 115) & (result_df["denominator_id"] == 3)]
        brand_118_category_3 = result_df[(result_df["numerator_id"] == 118) & (result_df["denominator_id"] == 3)]

        self.assert_kpi_result_equal(kpi_result=brand_110_category_3, numerator_id=110)
        self.assert_kpi_result_equal(kpi_result=brand_113_category_3, numerator_id=113)
        self.assert_kpi_result_equal(kpi_result=brand_115_category_3, numerator_id=115)
        self.assert_kpi_result_equal(kpi_result=brand_118_category_3, numerator_id=118)
        self.assertEqual(len(result_df), 4)

    def assert_kpi_result_equal(self, kpi_result, numerator_id=None):
        """
        :param kpi_result: The kpi result
        :type kpi_result:
        :return:
        """
        self.assertEquals(kpi_result['numerator_id'].get_values()[0], numerator_id)