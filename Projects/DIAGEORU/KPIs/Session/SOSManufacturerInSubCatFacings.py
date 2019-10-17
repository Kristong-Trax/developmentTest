from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSPerSubCategoryInStore
from Projects.DIAGEORU.KPIs.util import SimpleFacingsRetriever


class DiageoFacingsManufacturerInSubCategorySOS(ManufacturerFacingsSOSPerSubCategoryInStore):
    @property
    def field_retriever(self):
        return SimpleFacingsRetriever(self._data_provider)
