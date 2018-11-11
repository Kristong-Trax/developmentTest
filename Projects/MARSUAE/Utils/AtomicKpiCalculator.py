from KPIUtils_v2.Calculations.KpiStructure.KpiBaseCalculation import KpiBaseCalculation
from Trax.Utils.DesignPatterns.Decorators import classproperty


__author__ = 'israel'


class SOSCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        pass

    def calculate(self, params):
        pass

    def calculate_result_and_write(self, params, result, numerator_result, denominator_result):
        common = self._data_provider.common
        points = params['Points'].iloc[0]
        kpi_fk = common.get_kpi_fk_by_kpi_type(params['Atomic KPI'].iloc[0])

        if result >= params['upper threshold'].iloc[0]:
            result = points
        elif result < params['lower threshold'].iloc[0]:
            result = 0
        else:
            result *= points

        return self._create_kpi_result(fk=kpi_fk, result=result, score=result,
                                        numerator_id=999, numerator_result=numerator_result,
                                        denominator_id=999, denominator_result=denominator_result)


class LinearSOSCalculation(SOSCalculation):
    @classproperty
    def kpi_type(self):
        return 'linear SOS'

    def calculate(self, params):
        numerator_filters = {params['numerator type'].iloc[0]: params['numerator value'].iloc[0]}
        general_filters = {params['denominator type'].iloc[0]: params['denominator value'].iloc[0]}

        result, numerator_result, denominator_result = \
            self._data_provider.sos.calculate_linear_share_of_shelf_with_numerator_denominator(
                sos_filters=numerator_filters,
                **general_filters)
        return self.calculate_result_and_write(params, result, numerator_result, denominator_result)


class DisplaySOSCalculation(SOSCalculation):
    @classproperty
    def kpi_type(self):
        return 'Display SOS'

    def calculate(self, params):
        sos_threshold = params['minimum threshold'].iloc[0]
        result = numerator_result = denominator_result = 0
        relevant_scenes = self.get_secondary_shelf_scenes()
        if len(relevant_scenes) > 0:
            mars_secondary_count = self.get_mars_secondary_shelf_scenes(relevant_scenes, sos_threshold)
            result = mars_secondary_count / self.get_scenes_weights(relevant_scenes)

        return self.calculate_result_and_write(params, result, numerator_result, denominator_result)

    def get_secondary_shelf_scenes(self):
        relevant_scenes = self._scif.loc[self._scif['template_group'] == 'Secondary Shelf']
        return relevant_scenes[['scene_id', 'additional_attribute_1']].drop_duplicates()

    def get_mars_secondary_shelf_scenes(self, relevant_scenes, sos_threshold):
        mars_displays = 0
        for scene in relevant_scenes['scene_id']:
            scene_weight = relevant_scenes[relevant_scenes['scene_id'] == scene]['additional_attribute_1'].iloc[0]
            scene_scif = self._scif[self._scif['scene_id'] == scene]
            mars_products = scene_scif[scene_scif['manufacturer_name'] == 'Mars'].facings
            if mars_products > 0:
                competitors_products = scene_scif[scene_scif['manufacturer_name'] != 'Mars'].facings
                if competitors_products > 0:
                    if mars_products / competitors_products >= sos_threshold:
                        mars_displays += scene_weight
                else:
                    mars_displays += scene_weight
        return mars_displays

    @staticmethod
    def get_scenes_weights(relevant_scenes):
        return sum(relevant_scenes['additional_attribute_1'])


class DistributionCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Distribution'

    def calculate(self, params):
        target = params['minimum products'].iloc[0]
        points = params['Points'].iloc[0]
        count_pass_product = 0
        result = 0

        kpi_fk = 1
        assortment_fk = self.get_assortment_group_fk(params['Assortment group'].iloc[0])
        assortment_result = self._data_provider.assortment.calculate_lvl3_assortment()
        assortment_result = assortment_result[assortment_result['assortment_group_fk'] == assortment_fk]
        for row in assortment_result.itertuples():
            product_result = self._create_kpi_result(fk=kpi_fk, numerator_id=row['product_fk'],
                                                     numerator_result=row['in_store'], score=row['in_store'] * 100,
                                                     result=self.get_result_value(row['in_store']))
            self._data_provider.common.write_to_db_result(**product_result)
            if row['in_store']:
                count_pass_product += 1

        if target:
            if count_pass_product >= target:
                result = points
        else:
            if result >= params['upper threshold'].iloc[0]:
                result = points
            elif result < params['lower threshold'].iloc[0]:
                result = 0
            else:
                result *= points

        return self._create_kpi_result(fk=kpi_fk, numerator_id=999, numerator_result=result, score=result)

    @staticmethod
    def get_assortment_group_fk(assortment_name):
        return 1

    def get_result_value(self, score):
        result = 4 if score is 1 else 5
        return result


class AvailabilityCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availability'

    def calculate(self, params):
        result = 0
        target = params['minimum products'].iloc[0]

        filters = {params['Type_1'].iloc[0]: params['Type_1'].iloc[0]}
        if params['Type_2'].iloc[0]:
            filters.update({params['Type_2'].iloc[0]: params['Type_2'].iloc[0]})
        filtered_scif = self._scif[self._toolbox.get_filter_condition(self._scif, **filters)]
        if not filtered_scif.empty:
            if sum(filtered_scif.facings) >= target:
                result = 100
            result *= params['Points'].iloc[0]

        return self._create_kpi_result(fk=self.kpi_fk, result=result, score=result, numerator_id=999, target=target,
                                        numerator_result=None, denominator_id=999, denominator_result=None)


class AggregationCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Aggregation'

    def calculate(self, params):
        result = params['score']
        return self._create_kpi_result(fk=self.kpi_fk, result=result, score=result, numerator_id=999,
                                        numerator_result=None, denominator_id=999, denominator_result=None)