from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.PSAssortmentProvider import LiveAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.Assortment import LiveAssortmentCalculation

class CalculateKpi(LiveSessionBaseClass):
    def calculate_session_live_kpi(self):
        data_provider = self._data_provider
        # store_assortment = PSAssortmentDataProvider(data_provider).execute()
        matche = data_provider.matches
        scif = data_provider.scene_item_facts
        assortment = LiveAssortmentCalculation(data_provider)
        a= assortment.get_lvl3_relevant_ass()
        b= assortment.calculate_lvl3_assortment(False)
        c = assortment.calculate_lvl2_assortment(b)

        print("ok")

    #
    # def assortment(self):
    #     data_provider = self._data_provider
