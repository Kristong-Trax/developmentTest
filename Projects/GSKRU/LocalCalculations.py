from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKRU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'gskru'
    sessions = [

        "08ca73f6-bbf5-4284-9155-e9e426819b2c",
        "08f18c18-6913-4ab6-9bc4-1135352dee69",
        "2e6d2f04-c902-4e13-ac0e-023fd2d97c89",
        "3FE38297-66F3-452A-8ECA-341FFFA77BE3",
        "5011c33f-763e-42ca-8702-cf9048b4743c",
        "5ccf5fb4-e496-4d5e-bd87-ac3ac81cc241",
        "5df98b7f-19cd-47a3-80bf-9dc5f229bed3",
        "81813e9c-b55d-415c-88dc-215ddf93a8cd",
        "ae3e5ff3-37a3-4352-98f1-e91e05e9ca31",
        "c272b55c-f403-4037-b9ae-8dfd8b3e0bce",
        "cfc6fbe0-7ec0-46b8-b7ed-12af306b4aa3",
        "f4747b24-d33a-4d94-b269-f28cde16cac1",

        # "fe116d3f-e1b9-49e4-9179-b061fa7c86a0",
        # "98c9862e-393e-40d1-97b8-af67892d2f1a",
        # "83453a69-6b52-44b7-92fc-574f00bdff2e",
        # "993cff34-c1b3-4659-95f5-44de06468f07"

        # 'f98497c6-1b30-4105-841b-0eafec5a7a9f',
        # 'aca2d140-d7dc-4817-9a42-95d2a69c70ff',
        # 'c61f1054-e11f-41c3-b21e-54f7a4f776d7',
        # '993cff34-c1b3-4659-95f5-44de06468f07',
        # '87873358-fcb2-40d6-b68d-f59c07242e76',
        # 'e161137e-5943-427c-acc2-2ecd30d0e16d',
        # 'fea0f508-cfa5-4968-be68-256575a122ea'

        ]

    for session in sessions:
        print session
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
