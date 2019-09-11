from OutOfTheBox.Calculations.SubCategorySOS import SubCategoryFacingsSOSPerCategory
from Projects.DIAGEORU.KPIs.util import SimpleFacingsRetriever


class DiageoFacingsSubCategoryInCategorySOS(SubCategoryFacingsSOSPerCategory):
    @property
    def field_retriever(self):
        return SimpleFacingsRetriever(self._data_provider)










