from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from OutOfTheBox.Calculations.SOSBase import SOSfCalculationBase, FacingsFieldRetriever
from Trax.Utils.DesignPatterns.Decorators import classproperty


class OwnManufacturerSecondaryFacingsSOSInStore(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @property
    def own(self):
        return True

    @classproperty
    def kpi_type(cls):
        return 'SOS OWN MANUFACTURER OUT OF STORE - SECONDARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'Y']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class CategorySecondaryFacingsSOSInStore(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS CATEGORY OUT OF STORE -  SECONDARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.CATEGORY_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'Y']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class ManufacturerSecondaryFacingsSOSPerCategory(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

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
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'Y']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class BrandOutManufacturerOutCategorySecondaryFacingsSOS(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - SECONDARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.BRAND_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.MANUFACTURER_FK

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.CATEGORY_FK

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'Y']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class OwnManufacturerPrimaryFacingsSOSInStore(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @property
    def own(self):
        return True

    @classproperty
    def kpi_type(cls):
        return 'SOS OWN MANUFACTURER OUT OF STORE - PRIMARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'N']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class CategoryPrimaryFacingsSOSInStore(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS CATEGORY OUT OF STORE -  PRIMARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.CATEGORY_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'N']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class ManufacturerPrimaryFacingsSOSPerCategory(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.MANUFACTURER_FK

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
        return Keys.STORE_ID

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'N']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos


class BrandOutManufacturerOutCategoryPrimaryFacingsSOS(SOSfCalculationBase):
    def __init__(self, data_provider, kpi_definition_fk, targets=None, action_calculators=None,
               cross_session_data_provider=None, categories_include=None):
        SOSfCalculationBase.__init__(self, data_provider, kpi_definition_fk, targets, action_calculators,
                                     cross_session_data_provider)
        self._category_to_include = categories_include

    @property
    def category_to_include(self):
        return self._category_to_include

    @property
    def field_retriever(self):
        return FacingsFieldRetriever(self._data_provider)

    @property
    def sos_field(self):
        return Fields.FACINGS

    @classproperty
    def kpi_type(cls):
        return 'SOS BRAND OUT OF MANUFACTURER OUT OF CATEGORY - PRIMARY SHELF'

    @classproperty
    def kpi_supported_actions(cls):
        return []

    @property
    def numerator_grouping_key(self):
        return Keys.BRAND_FK

    @property
    def denominator_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.MANUFACTURER_FK

    @property
    def context_grouping_key(self):
        """
        :rtype: int
        """
        return Keys.CATEGORY_FK

    def _get_scene_item_facts_filtered_by_primary_secondary_scene(self, scene_item_facts):
        return scene_item_facts[scene_item_facts[Fields.INCLUDED_IN_SECONDARY_SHELF_REPORT] == 'N']

    def _get_scene_item_facts_filtered_by_category(self, scene_item_facts):
        filtered_scif_by_category = scene_item_facts[scene_item_facts[Fields.CAT].isin(self.category_to_include)]
        return filtered_scif_by_category

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_scene_type = self._get_scene_item_facts_filtered_by_primary_secondary_scene(scene_item_facts)
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(filtered_scif_by_scene_type)
        filtered_scif_by_category = self._get_scene_item_facts_filtered_by_category(filtered_scif_by_product_type)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_category)
        return filtered_scif_rlv_sos
