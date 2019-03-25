from Trax.Utils.Testing.Case import skip

from Projects.GFKDE.ShareOfBrandedZonesBrandCategory import ShareOfBrandedZonesBrandCategory_KPI
from Projects.GFKDE.Tests.Sessions_Data import bz_data
from Trax.Algo.Calculations.OutOfTheBox.Calculations.Tests.Base import BaseCalculationTest


class TestWashingMachinesSOSBrandCategory(BaseCalculationTest):
    def import_path(self):
        return 'Projects.GFKDE.SOSUnboxedBrandCategory.ShareOfBrandedZonesBrandCategory_KPI'

    def set_up(self):
        super(TestWashingMachinesSOSBrandCategory, self).set_up()
        self._kpi_definition_fk = 10

    @skip('fail')
    def test_calculate_return_resuls(self):
        self._mock_scene_item_facts(data=bz_data)
        calculation = ShareOfBrandedZonesBrandCategory_KPI(self._data_provider_mock, self._kpi_definition_fk)
        kpi_type, result_df = calculation.calculate()
        brand_113_category_3 = result_df[(result_df["numerator_id"]== 113) & (result_df["denominator_id"]== 3)]
        brand_118_category_3 = result_df[(result_df["numerator_id"] == 118) & (result_df["denominator_id"] == 3)]
        brand_152_category_3 = result_df[(result_df["numerator_id"] == 152) & (result_df["denominator_id"] == 3)]

        self.assert_kpi_result_equal(kpi_result=brand_113_category_3, numerator_id=113, numerator_result=1,
                                    denominator_id=3, denominator_result=3, result=0.333)
        self.assert_kpi_result_equal(kpi_result=brand_118_category_3, numerator_id=118, numerator_result=1,
                                     denominator_id=3, denominator_result=3, result=0.333)
        self.assert_kpi_result_equal(kpi_result=brand_152_category_3, numerator_id=152, numerator_result=1,
                                     denominator_id=3, denominator_result=3, result=0.333)

    @skip('fail')
    def assert_kpi_result_equal(self, kpi_result, numerator_id=None, numerator_result=None, denominator_id=None, denominator_result=None, result=None):
        """
        :param kpi_result: The kpi result
        :type kpi_result:
        :return:
        """
        self.assertEquals(kpi_result['numerator_id'].get_values()[0], numerator_id)
        self.assertEquals(kpi_result['numerator_result'].get_values()[0], numerator_result)
        self.assertEquals(kpi_result['denominator_id'].get_values()[0], denominator_id)
        self.assertEquals(kpi_result['denominator_result'].get_values()[0], denominator_result)
        self.assertAlmostEquals(kpi_result['result'].get_values()[0], result, places=2)
