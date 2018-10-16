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
        points = params['Points']
        kpi_fk = common.get_kpi_fk_by_kpi_type(params['Atomic KPI'])

        sos = SOS(self._data_provider, output=None)
        result, numerator_result, denominator_result = sos.calculate_linear_share_of_shelf_with_numerator_denominator(
                                                                                        sos_filters=numerator_filters,
                                                                                        **general_filters)
        if result >= params['upper threshold']:
            result = points
        elif result < params['upper threshold']:
            result = 0
        else:
            result *= points

        return [self._create_kpi_result(fk=kpi_fk, result=result, score=result,
                                        numerator_id=999, numerator_result=numerator_result,
                                        denominator_id=999, denominator_result=denominator_result)]


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


class AvailabilityBaseCalculation(KpiBaseCalculation):
    def calculate(self, params):
        pass

    @classproperty
    def kpi_type(self):
        pass

    def calculate_availability(self, params, attribute_type):
        result = 0
        target = params['minimum products']
        filters = {attribute_type: params.iloc[0]['scene type'].split(',')}
        scif = self._data_provider.scene_item_facts
        scif = scif[self._toolbox.get_filter_condition(scif, **filters)]
        if not scif.empty:
            if sum(scif.facings) >= target:
                result = 100
            result *= params['Points']

        return self._create_kpi_result(fk=self.kpi_fk, result=result, score=result,
                                        numerator_id=999, numerator_result=scif,
                                        denominator_id=999, denominator_result=None,
                                        target=target)


class AvailabilityHangingStripCalculation(AvailabilityBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availability hanging strip'

    def calculate(self, params):
        return self.calculate_availability(params=params, attribute_type='template_name')


class AvailabilityBasketCalculation(AvailabilityBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availability basket'

    def calculate(self, params):
        return self.calculate_availability(params=params, attribute_type='product_type')


class AvailabilityMultipackCalculation(AvailabilityBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availability multipack'

    def calculate(self, params):
        return self.calculate_availability(params=params, attribute_type='package')