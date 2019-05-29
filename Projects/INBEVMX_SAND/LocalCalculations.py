
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVMX_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevmx-sand calculations')
    Config.init()
    project_name = 'inbevmx-sand'
    data_provider = KEngineDataProvider(project_name)
    list_sessions = [
                    'fe01d68e-a6b5-4ba2-bdf5-a5aa7d47eb40',
                    "FC8C65F2-5DE5-4440-9B91-FF9EF6CB302B",
                    "2ecbf3fa-59b9-46bf-86b8-2b11fe49d4e9",
                    "ffcecdd2-7eda-446b-af6e-cee7628b89ed",
                    "6b4fc678-1f82-45c7-a1d3-3bf85ea4c013",
                    "fac755c5-c53b-40f3-832d-62c13cacec1e",
                    "113ad721-6fae-4f13-ab64-19e00304abef"



                # 'ffbeae34-949f-457d-99ac-6b99260ac743',
                # 'fb5ca6e2-5f9f-4fec-8f2d-c9e6a3536219',
                # '15E6D6FD-FC45-4703-8A99-D33C805FAC41',
                # 'ffd0da53-6fd6-418f-b06d-f5cbff735750',
                # 'fff20792-6a60-4a13-bb00-879a308c1ea6',
                # 'ffbbcecc-5862-4e47-80ef-230f4ba97f8d',
                # 'c14c74c6-d507-4681-ac77-9c1dfd9b8c3b',
                # 'fff20792-6a60-4a13-bb00-879a308c1ea6',
                # '341a9b53-65ad-43b6-9fe0-2ae56fcbe9bd',
                # '6a19080f-9741-4760-85fb-22a5e774d13b'
            ]
    output = Output()
    for session in list_sessions:
        data_provider.load_session_data(session)
        Calculations(data_provider, output).run_project_calculations()
