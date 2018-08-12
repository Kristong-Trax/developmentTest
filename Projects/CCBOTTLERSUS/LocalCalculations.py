
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        '7c5284d4-93e0-46e2-a31c-75075d2323e0', # CR&LT
        '55d5c959-cb08-477b-9f9e-0af4fa9f3795', # DRUG
        '714f5168-b9d9-4f9b-8f3e-3a0723c68253', # VALUE

        # "c2b4723f-ea1b-456d-9647-48ef779cfcb8",
        # "9e0cd962-74b6-48ac-ba13-6e674c198ea3",
        # "86997b82-e7e4-4155-91f5-9cd30de7b55c",
        # "402bb0f7-7e58-4532-94a5-21ed2538d2e6",
        # "15283f33-65f7-4abf-91e2-084801ec4c61",
        # "11044558-fc7f-4882-8243-e301528aa5e8",
        #
        # "c8a622c9-7f64-4242-b922-4d85fa7d935b",
        # "a620964b-9385-43cf-98c8-25ac7043402b",
        # "6b64188d-4004-4116-90af-f76bf2a9f3d5",
        # "5396fc69-5304-4e77-a18e-828d521666d5",
        # "2047cb29-04a8-49ee-988b-bc70b52cf92a",
        # "0dfe8c86-65b8-43de-93d4-9e417cae0e74",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
