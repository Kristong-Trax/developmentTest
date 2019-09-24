from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSInWholeStore
from Projects.DIAGEORU.KPIs.util import SimpleFacingsRetriever


class DiageoFacingsManufacturerInStoreSOS(ManufacturerFacingsSOSInWholeStore):
    @property
    def field_retriever(self):
        return SimpleFacingsRetriever(self._data_provider)




