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
        numerator_filters = {params['numerator type'].iloc[0]: params['numerator value'].iloc[0]}
        general_filters = {params['denominator type'].iloc[0]: params['denominator value'].iloc[0]}
        points = params['Points'].iloc[0]
        kpi_fk = common.get_kpi_fk_by_kpi_type(params['Atomic KPI'].iloc[0])

        sos = SOS(self._data_provider, output=None)
        result, numerator_result, denominator_result = sos.calculate_linear_share_of_shelf_with_numerator_denominator(
                                                                                        sos_filters=numerator_filters,
                                                                                        **general_filters)
        if result >= params['upper threshold'].iloc[0]:
            result = points
        elif result < params['upper threshold'].iloc[0]:
            result = 0
        else:
            result *= points

        return [self._create_kpi_result(fk=kpi_fk, result=result, score=result,
                                        numerator_id=999, numerator_result=numerator_result,
                                        denominator_id=999, denominator_result=denominator_result)]


class DisplaySOSCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Display SOS'

    def calculate(self, params):
        common = self._data_provider.common
        points = params['Points'].iloc[0]
        sos_threshold = params[''].iloc[0]
        kpi_fk = common.get_kpi_fk_by_kpi_type(params['Atomic KPI'].iloc[0])
        scif = self._data_provider.scene_item_facts

        result = numerator_result = denominator_result = 0
        relevant_scenes = self.get_secondary_shelf_scenes(scif)
        if len(relevant_scenes) > 0:
            mars_secondary_count = self.get_mars_secondary_shelf_scenes(scif, relevant_scenes, sos_threshold)
            result = mars_secondary_count / len(relevant_scenes)

            if result >= params['upper threshold'].iloc[0]:
                result = points
            elif result < params['upper threshold'].iloc[0]:
                result = 0
            else:
                result *= points

        return [self._create_kpi_result(fk=kpi_fk, result=result, score=result,
                                        numerator_id=999, numerator_result=numerator_result,
                                        denominator_id=999, denominator_result=denominator_result)]

    @staticmethod
    def get_secondary_shelf_scenes(scif):
        return scif.loc[scif['template_group'] == 'Secondary Shelf']['scene_id'].unique().tolist()

    @staticmethod
    def get_mars_secondary_shelf_scenes(scif, relevant_scenes, sos_threshold):
        mars_displays = 0
        for scene in relevant_scenes:
            scene_scif = scif[scif['scene_id'] == scene]
            mars_products = scene_scif[scene_scif['manufacturer_name'] == 'Mars'].facings
            if mars_products > 0:
                competitors_products = scene_scif[scene_scif['manufacturer_name'] != 'Mars'].facings
                if competitors_products > 0:
                    if mars_products / competitors_products >= sos_threshold:
                        mars_displays += 1
                else:
                    mars_displays += 1
        return mars_displays


class DistributionCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Distribution'

    def calculate(self, params):
        result_kpi = []
        kpi_fk = 1
        assortment_fk = self.get_assortment_group_fk(params['Assortment group'].iloc[0])
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
        target = params['minimum products'].iloc[0]

        filters = {params['Type_1'].iloc[0]: params['Type_1'].iloc[0]}
        if params['Type_2'].iloc[0]:
            filters.update({params['Type_2'].iloc[0]: params['Type_2'].iloc[0]})
        # filters = {attribute_type: params.iloc[0]['scene type'].split(',')}
        scif = self._data_provider.scene_item_facts
        scif = scif[self._toolbox.get_filter_condition(scif, **filters)]
        if not scif.empty:
            if sum(scif.facings) >= target:
                result = 100
            result *= params['Points'].iloc[0]

        return [self._create_kpi_result(fk=self.kpi_fk, result=result, score=result, numerator_id=999, target=target,
                                        numerator_result=None, denominator_id=999, denominator_result=None)]


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