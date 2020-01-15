from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider


class LiveAssortmentDataProvider(PSAssortmentDataProvider):
    def __init__(self, data_provider):
        PSAssortmentDataProvider.__init__(self, data_provider)

