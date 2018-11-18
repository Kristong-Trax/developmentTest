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
        points = float(params['Points'].iloc[0])
        score = 0

        if result >= float(params['upper threshold'].iloc[0]):
            score = points
        elif result < float(params['lower threshold'].iloc[0]):
            score = 0
        else:
            score *= points

        return self._create_kpi_result(fk=self.kpi_fk, result=result * 100, score=score, weight=points,
                                       numerator_id=3, numerator_result=numerator_result,
                                       denominator_id=self._data_provider.store_fk,
                                       denominator_result=denominator_result)


class LinearSOSCalculation(SOSCalculation):
    @classproperty
    def kpi_type(self):
        return 'linear SOS'

    def calculate(self, params):
        numerator_filters = {params['numerator type'].iloc[0]:
                             self.split_and_strip(params['numerator value'].iloc[0])}
        general_filters = {params['denominator type'].iloc[0]:
                           self.split_and_strip(params['denominator value'].iloc[0])}

        result, numerator_result, denominator_result = \
            self._data_provider.sos.calculate_linear_share_of_shelf_with_numerator_denominator(
                sos_filters=numerator_filters,
                **general_filters)
        return self.calculate_result_and_write(params, result, numerator_result, denominator_result)

    @staticmethod
    def split_and_strip(param):
        return map(lambda x: x.strip(), param.split(','))


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
        points = float(params['Points'].iloc[0])
        product_total_in_assortment = result = 0

        kpi_sku = self._data_provider.common_v2.get_kpi_fk_by_kpi_type(params['Atomic KPI'].iloc[0]+'_SKU')
        assortment_fk = self.get_assortment_group_fk(params['Assortment group'].iloc[0], self._data_provider)

        if kpi_sku and assortment_fk:
            assortment_result = self.get_assortment_result(assortment_fk)
            count_pass_product = self.write_to_db_per_sku_and_count_pass(kpi_sku, assortment_result, self.kpi_fk)
            result = self.check_result_vs_threshold(params, count_pass_product)
            product_total_in_assortment = len(assortment_result)
        # else:
        #     log.debug('Assortment name: {} not exists in DB'.format(params['Assortment group'].iloc[0]))
        return self._create_kpi_result(fk=self.kpi_fk, numerator_id=3,
                                       numerator_result=result, denominator_id=self._data_provider.store_fk,
                                       denominator_result=product_total_in_assortment,
                                       score=result, result=result, weight=points, target=target)

    @staticmethod
    def get_assortment_group_fk(assortment_name, data_provider):
        return data_provider.assortment.get_assortment_fk_by_name(assortment_name)

    @staticmethod
    def get_result_value(score):
        result = 4 if score is 1 else 5
        return result

    def get_assortment_result(self, assortment_fk):
        import json
        assortment_result = self._data_provider.assortment.get_lvl3_relevant_ass()
        assortment_result = assortment_result[assortment_result['assortment_group_fk'] == assortment_fk]
        scene_type = json.loads(assortment_result.iloc[0]['additional_attributes']).get('scene_type')
        scif = self._data_provider.scene_item_facts.copy()
        products_in_session = scif.loc[scif['facings'] > 0]
        products_in_session = products_in_session[products_in_session['template_name'] == scene_type][
            'product_fk'].values
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
        return assortment_result

    def write_to_db_per_sku_and_count_pass(self, kpi_sku, assortment_result, parent_level_2_identifier):
        count_pass_product = 0
        for i, row in assortment_result.iterrows():
            product_result = self._create_kpi_result(fk=kpi_sku, numerator_id=row['product_fk'],
                                                     numerator_result=row['in_store'], score=row['in_store'] * 100,
                                                     result=self.get_result_value(row['in_store']))
            product_result.update({'identifier_parent': parent_level_2_identifier,
                                   'should_enter': True})
            self._data_provider.common_v2.write_to_db_result(**product_result)
            if row['in_store']:
                count_pass_product += 1
        return count_pass_product

    @staticmethod
    def check_result_vs_threshold(params, count_pass_product):
        target = params['minimum products'].iloc[0]
        points = float(params['Points'].iloc[0])
        result = 0
        if target:
            if count_pass_product >= float(target):
                result = points
        else:
            if result >= float(params['upper threshold'].iloc[0]):
                result = points
            elif result < float(params['lower threshold'].iloc[0]):
                result = 0
            else:
                result *= points
        return result


class AvailabilityCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Availability'

    def calculate(self, params):
        result = score = 0
        target = float(params['minimum products'].iloc[0])
        points = float(params['Points'].iloc[0])

        filters = {params['Type_1'].iloc[0]: self.split_and_strip(params['Value_1'].iloc[0]),
                   'template_name': self.split_and_strip(params['scene type'].iloc[0])}
        if params['Type_2'].iloc[0]:
            filters.update({params['Type_2'].iloc[0]: self.split_and_strip(params['Value_2'].iloc[0])})
        filtered_scif = self._scif[self._toolbox.get_filter_condition(self._scif, **filters)]
        if not filtered_scif.empty:
            if sum(filtered_scif.facings) >= target:
                result = score = 100
            score *= points

        return self._create_kpi_result(fk=self.kpi_fk, result=result, score=score,
                                       numerator_id=3, target=target, numerator_result=None,
                                       denominator_id=self._data_provider.store_fk,
                                       denominator_result=None, weight=points)

    @staticmethod
    def split_and_strip(param):
        return map(lambda x: x.strip(), param.split(','))


class AggregationCalculation(KpiBaseCalculation):
    @classproperty
    def kpi_type(self):
        return 'Aggregation'

    def calculate(self, params):
        result = params['score']
        potential = params['potential']
        return self._create_kpi_result(fk=self.kpi_fk, result=result, score=result,
                                       numerator_id=3, weight=potential,
                                       numerator_result=None, denominator_id=self._data_provider.store_fk,
                                       denominator_result=None)
