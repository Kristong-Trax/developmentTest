from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from OutOfTheBox.Calculations.SOSBase import SOSfCalculationBase, FacingsFieldRetriever
from Trax.Utils.DesignPatterns.Decorators import classproperty


class OwnManufacturerSOSInStore(SOSfCalculationBase):
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
        return 'SOS_BY_OWN_MAN'

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

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(scene_item_facts)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_product_type)
        return filtered_scif_rlv_sos

##########################################################################################################################
class ManufacturerFacingsSOSPerCategory(SOSfCalculationBase):
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
        return 'SOS_BY_OWN_MAN_CAT'

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

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(scene_item_facts)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_product_type)
        return filtered_scif_rlv_sos


class BrandOutManufacturerOutCategoryFacingsSOS(SOSfCalculationBase):

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
        return 'SOS_BY_OWN_MAN_CAT_BRAND'

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
        return Keys.CATEGORY_FK

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(scene_item_facts)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_product_type)
        return filtered_scif_rlv_sos


class SkuOutBrandOutCategoryFacingsSOS(SOSfCalculationBase):

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
        return 'SOS_BY_OWN_MAN_CAT_BRAND_SKU'

    @classproperty
    def kpi_supported_actions(cls):
        return []

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
        return Keys.CATEGORY_FK

    def _get_filtered_scene_item_facts(self, scene_item_facts):
        filtered_scif_by_product_type = self._get_scene_item_facts_filtered_by_product_type(scene_item_facts)
        filtered_scif_rlv_sos = self._get_scene_item_facts_filtered_by_rlv_sos(filtered_scif_by_product_type)
        return filtered_scif_rlv_sos
