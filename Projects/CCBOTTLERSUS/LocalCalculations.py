
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        "55d5c959-cb08-477b-9f9e-0af4fa9f3795",
        "7c5284d4-93e0-46e2-a31c-75075d2323e0",
        "714f5168-b9d9-4f9b-8f3e-3a0723c68253",
        "D8AC45CA-252D-4490-805C-37FF601AC7EC",
        "a8e23604-4ef0-48c8-bd89-563d8d687441",
        "b0aabe46-724b-466f-be0d-56caf82d6667",
        "546AAB1A-259D-4EDA-909E-D8AC9E89D4AD",
        "D3D3E61E-F595-4D9D-9B5F-7188321420E1",
        "bc8a1244-a53d-4b2f-9591-fcf57f9d3054",

        "fb96c8dd-7c4a-4b53-b32a-4d1e5e5bec33",
        "f7015dbf-1bd8-4574-b75d-0fb25cc561b9",
        "ee2b0cf4-2153-4e71-937e-27552a057058",
        "eb67dd5e-dc40-44df-bfd2-f0fe301fa178",
        "e860b343-4e10-4ca7-9c09-1b416ff5699f",

        "171ebeab-60f0-4df3-8403-f83bf519f6bd",
        "FE678C7C-DAF5-4906-9279-2F4490EDD5F9",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
