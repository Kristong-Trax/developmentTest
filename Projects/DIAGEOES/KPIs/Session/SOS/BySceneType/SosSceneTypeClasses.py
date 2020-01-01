
from OutOfTheBox.Actions.Calculators.ManufacturerSos import SosManufacturerCategorySceneTypeKpiAction
from OutOfTheBox.Calculations.SOSBase import FacingsFieldRetriever, SOSfCalculationBase, LinearFieldRetriever, \
    LinearGrossFieldRetriever, LinearNetFieldRetriever
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, ConfigurationParameters as Cp
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ManufacturerFacingsSOSbySceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS OUT OF STORE BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK


class SubCategoryFacingsSOSPerCategoryInSceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS SUB CATEGORY OUT OF CATEGORY BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.SUB_CATEGORY_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.CATEGORY_FK

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK


class ManufacturerFacingsSOSPerSubCategoryInSceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS MANUFACTURER OUT OF SUB CATEGORY BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.SUB_CATEGORY_FK

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK


class StoreFacingsSOSPerCategoryInSceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS CATEGORY OUT OF STORE BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.CATEGORY_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK


class ManufacturerFacingsSOSBrandPerSubCategoryInSceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS BRAND OUT OF SUB CATEGORY BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.BRAND_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.SUB_CATEGORY_FK
    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK

    def _calculate_results(self, scene_item_facts):

        measurement_unit = None
        try:
            measurement_unit = self._data_provider.get_configuration_param_value(
                Cp.ParamNames.SOS_MEASUREMENT_UNIT_FROM_MM)
        except Exception as e1:
            pass

        if measurement_unit and type(self.field_retriever) in [LinearFieldRetriever, LinearGrossFieldRetriever,
                                                               LinearNetFieldRetriever]:
            measurement_unit = float(measurement_unit)
        else:
            measurement_unit = 1

        denominator_results = \
            scene_item_facts.groupby(('template_fk', 'sub_category_fk', 'manufacturer_fk'), as_index=False)[
                [self.field_retriever.sos_field]].sum().rename(
                columns={self.field_retriever.sos_field: Keys.DENOMINATOR_RESULT})

        if self.own:
            scene_item_facts = scene_item_facts[scene_item_facts[Keys.MANUFACTURER_FK].isin(
                self._data_provider.own_manufacturer[Fields.PARAM_VALUE].values)]

        numerator_result = scene_item_facts.groupby(('brand_fk', 'template_fk', 'sub_category_fk',
                                                     'manufacturer_fk'), as_index=False)[
            [self.field_retriever.sos_field]].sum().rename(
            columns={self.field_retriever.sos_field: Keys.NUMERATOR_RESULT})

        results = numerator_result.merge(denominator_results)
        results[Keys.RESULT] = ((results[Keys.NUMERATOR_RESULT] / results[Keys.DENOMINATOR_RESULT])
                                * measurement_unit)

        return results

    def _convert_to_kpi_results(self, results):
        scope_id = self._data_provider.current_scope_id
        calculation_results = []
        for index, row in results.iterrows():
            numerator_id = row[self.numerator_grouping_key]
            denominator_id = row[self.denominator_grouping_key]
            context_id = row[self.context_grouping_key] if self.context_grouping_key is not None else None

            target = self.targets_calculator.get_target(self.targets, scope_id, numerator_id=numerator_id,
                                                        denominator_id=denominator_id, context_id=context_id)
            kpi_result = {"numerator_id": numerator_id, "result": row[Keys.RESULT],
                          "numerator_result": row[Keys.NUMERATOR_RESULT], "denominator_id": denominator_id,
                          "denominator_result": row[Keys.DENOMINATOR_RESULT], "weight": None,
                          "context_id": context_id, "kpi_definition_fk": self.kpi_definition_fk,
                          "score": row[Keys.RESULT], "score_after_actions": row[Keys.RESULT], "target": target,
                          "identifier_parent": (int(row['manufacturer_fk']), int(row['sub_category_fk'])),
                          "identifier_result": (int(row['manufacturer_fk']), int(row['sub_category_fk']),
                                                int(row['brand_fk']))}

            calculation_results.append(kpi_result)

        return calculation_results


class ManufacturerFacingsSOSProductPerBrandInSceneType(SOSfCalculationBase):
    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @classproperty
    def kpi_type(cls):
        return 'SOS PRODUCT OUT OF BRAND BY SCENE TYPE'

    @classproperty
    def kpi_supported_actions(cls):
        return [SosManufacturerCategorySceneTypeKpiAction]

    @property
    def numerator_grouping_key(self):
        return Keys.PRODUCT_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.BRAND_FK
    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.TEMPLATE_FK

    def _calculate_results(self, scene_item_facts):

        measurement_unit = None
        try:
            measurement_unit = self._data_provider.get_configuration_param_value(
                Cp.ParamNames.SOS_MEASUREMENT_UNIT_FROM_MM)
        except Exception as e1:
            pass

        if measurement_unit and type(self.field_retriever) in [LinearFieldRetriever, LinearGrossFieldRetriever,
                                                               LinearNetFieldRetriever]:
            measurement_unit = float(measurement_unit)
        else:
            measurement_unit = 1

        denominator_results = \
            scene_item_facts.groupby(('brand_fk','template_fk', 'sub_category_fk', 'manufacturer_fk'), as_index=False)[
                [self.field_retriever.sos_field]].sum().rename(
                columns={self.field_retriever.sos_field: Keys.DENOMINATOR_RESULT})

        if self.own:
            scene_item_facts = scene_item_facts[scene_item_facts[Keys.MANUFACTURER_FK].isin(
                self._data_provider.own_manufacturer[Fields.PARAM_VALUE].values)]

        numerator_result = scene_item_facts.groupby(('product_fk', 'brand_fk', 'template_fk', 'sub_category_fk',
                                                     'manufacturer_fk'), as_index=False)[
            [self.field_retriever.sos_field]].sum().rename(
            columns={self.field_retriever.sos_field: Keys.NUMERATOR_RESULT})

        results = numerator_result.merge(denominator_results)
        results[Keys.RESULT] = ((results[Keys.NUMERATOR_RESULT] / results[Keys.DENOMINATOR_RESULT])
                                * measurement_unit)

        return results

    def _convert_to_kpi_results(self, results):
        scope_id = self._data_provider.current_scope_id
        calculation_results = []
        for index, row in results.iterrows():
            numerator_id = row[self.numerator_grouping_key]
            denominator_id = row[self.denominator_grouping_key]
            context_id = row[self.context_grouping_key] if self.context_grouping_key is not None else None

            target = self.targets_calculator.get_target(self.targets, scope_id, numerator_id=numerator_id,
                                                        denominator_id=denominator_id, context_id=context_id)
            kpi_result = {"numerator_id": numerator_id, "result": row[Keys.RESULT],
                          "numerator_result": row[Keys.NUMERATOR_RESULT], "denominator_id": denominator_id,
                          "denominator_result": row[Keys.DENOMINATOR_RESULT], "weight": None,
                          "context_id": context_id, "kpi_definition_fk": self.kpi_definition_fk,
                          "score": row[Keys.RESULT], "score_after_actions": row[Keys.RESULT], "target": target,
                          "identifier_parent": (int(row['manufacturer_fk']), int(row['sub_category_fk']),
                                                int(row['brand_fk']))}

            calculation_results.append(kpi_result)

        return calculation_results
