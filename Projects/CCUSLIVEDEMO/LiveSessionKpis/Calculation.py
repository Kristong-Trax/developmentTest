from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.PSAssortmentProvider import LiveAssortmentDataProvider


class CalculateKpi(LiveSessionBaseClass):
    def calculate_session_live_kpi(self):
        data_provider = self._data_provider
        store_assortment = PSAssortmentDataProvider(data_provider).execute()
        scif = data_provider.scene_item_facts
        matche = data_provider.matches

        print("ok")

    #
    # def assortment(self):
    #     data_provider = self._data_provider
