
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JRIJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jrijp calculations')
    Config.init()
    project_name = 'jrijp'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
        # 'EBA5C272-6360-461A-A14C-9383008AEB25',  # no scenes
        # '90e21abd-f9b0-4fed-b13d-439721f83f7e',  # no scenes
        # '74b4993a-f13f-11e8-9ba7-1253817b9c00',  # no scene item facts
        "21DC16FA-3372-46B0-8B68-385AE16217A8",  # 2019-07-02
        "227AE743-AB78-4AB3-A731-36B1BE9D1999",  # 2019-07-02 -- problem with kpi data provider and mysql; scene 183676
        "29ED73AD-9838-40C5-AF9C-A25590B97B82",  # 2019-07-02
        "3C2E529E-B671-4602-99DE-66577DB9C0A3",  # 2019-07-02
        "4BF823FE-8F9E-43E3-9D2E-8E8F0BD1CA42",  # 2019-07-02
        "5B2B8FF3-2610-46B6-B617-A2EC636C8E14",  # 2019-07-02
        "8DCD4B11-A081-4EF2-8939-746F3BC11F86",  # 2019-07-02
        "9F83D11E-48AE-4AA1-BD7F-64B528AF51F9",  # 2019-07-02
        "A5E13401-BFBA-41E7-A20D-3400C0AFF316",  # 2019-07-02
        "AE042AD8-2B3E-4FB8-BC47-8D1D0006A834",  # 2019-07-02
        "BA7346E1-2C53-4062-92A7-E8CA92E93F02",  # 2019-07-02
        "E19D7F66-509A-4AFB-B562-D8113746104F",  # 2019-07-02
        "E5AAE11B-FBF8-4466-B9B4-B632B6EDC1DE",  # 2019-07-02
        "E8A721BE-3616-422F-BBC0-3629EA1BE84F",  # 2019-07-02
        # "DB8BED93-422D-44DB-AB6A-E04390E22F12",  # no posms in any scene
        # "AF4413D4-C5D5-4146-8E98-2E1A5F6414FB",
        # "F92FD08C-48B4-4756-AFB6-FF73F75C174F",
        # "FD372C87-6C7C-40F4-A6B3-D4BB7E988160",
        # "FE099D37-01D3-4D07-A6FA-8197FB98CB6E",
        # "FDBDA20B-479B-4A04-8036-F399B0957621",
        # "DFD37CE8-8A12-43D8-B466-E8C64E6E5B72",
        # "F5EA0101-2AF0-4B8B-B938-8615230FC1DF",
        # "6E838362-A0D9-4953-ABD0-2A1BA8783163",
        # "56233812-9FEA-4A0F-9F98-632EEB114F96",
        # "F9D9C7C8-2D7A-481C-8210-1E7906B78D93",
        # "E8A57D54-4B31-4197-BCEC-8448E2595A71",
        # "DAACC387-4209-4FDD-8E57-A4A19B3C7999",
    ]

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
