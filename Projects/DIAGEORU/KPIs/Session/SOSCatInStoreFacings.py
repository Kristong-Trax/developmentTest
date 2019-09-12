from OutOfTheBox.Calculations.SOSBase import SOSfCalculationBase
from Projects.DIAGEORU.KPIs.util import SimpleFacingsRetriever
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts


class CategoryFacingsSOSInWholeStore(SOSfCalculationBase):
    def kpi_type(self):
        pass

    def kpi_supported_actions(self):
        return []

    def field_retriever(self):
        return SimpleFacingsRetriever(self._data_provider)

    def numerator_grouping_key(self):
        return ScifConsts.CATEGORY_FK

    def denominator_grouping_key(self):
        return ScifConsts.STORE_ID
