from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.KpiStructure.KpiBaseCalculation import KpiBaseCalculation
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from Trax.Utils.DesignPatterns.Decorators import classproperty

# from Projects.MARSUAE.Utils.Const import Const

__author__ = 'israel'


class CountCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'SESSION_PARENT_1'

    def calculate(self, params):
        return [self._create_kpi_result(fk=1, numerator_id=1, denominator_id=3),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=2),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=1, context_id=1)]


class LinearSOSCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'linear SOS'

    def calculate(self, params):
        common = self._data_provider.common
        numerator_filters = {params['numerator type']: params['numerator value']}
        general_filters = {params['denominator type']: params['denominator value']}
        kpi_fk = common.get_kpi_fk_by_kpi_type(params['Atomic KPI'])

        result = SOS(self._data_provider, output=None).calculate_linear_share_of_shelf(sos_filters=numerator_filters,
                                                                                       **general_filters)
        common.write_to_db_result(fk=kpi_fk, result=result, score=result,
                                  numerator_id=999, numerator_result=numerator_score,
                                  denominator_id=999, denominator_result=denominator_score)
        return [self._create_kpi_result(fk=1, numerator_id=1, denominator_id=3),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=2),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=1, context_id=1)]


class DistributionSkuCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'SESSION_PARENT_1'

    def calculate(self, params):
        result_kpi = []
        kpi_fk = 1
        assortment_fk = params['assortment_group']
        assortment_result = Assortment(self, self._data_provider).calculate_lvl3_assortment()
        assortment_result = assortment_result[assortment_result['assortment_group_fk'] == assortment_fk]
        for i, row in assortment_result.itterows():
            result_kpi.append(self._create_kpi_result(fk=self._kpi_definition_fk,
                                                      numerator_id=row['product_fk'],
                                                      score=row['in_store']))
        return result_kpi


class DistributionCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Distribution'

    def calculate(self, params):
        result_kpi = []
        kpi_fk = 1
        assortment_fk = self.get_assortment_group_fk(params['Assortment group'])
        assortment_result = Assortment(data_provider=self._data_provider).calculate_lvl3_assortment()
        assortment_result = assortment_result[assortment_result['assortment_group_fk'] == assortment_fk]
        for i, row in assortment_result.iterrows():
            result_kpi.append(self._create_kpi_result(fk=kpi_fk, numerator_id=row['product_fk'], score=row['in_store']))
        return result_kpi

    def get_assortment_group_fk(self, assortment_name):
        return 1


class AvailabilityHangingStripCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availbility hanging strip'

    def calculate(self, params):
        return [self._create_kpi_result(fk=1, numerator_id=1, denominator_id=3),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=2),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=1, context_id=1)]


class AvailabilityFacingCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availbility facings (basket & Multipack SKUs)'

    def calculate(self, params):
        return [self._create_kpi_result(fk=1, numerator_id=1, denominator_id=3),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=2),
                self._create_kpi_result(fk=1, numerator_id=1, denominator_id=1, context_id=1)]
