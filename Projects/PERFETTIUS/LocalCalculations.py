
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PERFETTIUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('perfettius calculations')
    Config.init()
    project_name = 'perfettius'
    data_provider = KEngineDataProvider(project_name)
    # session_list = ['fcb4313c-9b91-40ed-966f-a1d35e0099ff']
    session_list = [
        "c887b586-4517-468d-a366-fb942aadd6a3",
        "963e277a-667c-40ca-94ce-90acdcf75c0d",
        "7278494e-c6f6-4e00-8fdb-ff9b0b1b9ef8",
        "304f8206-eee9-4559-9f64-3bfd75355f76",
        "1d0b2814-b100-4095-8b41-1cc8843a6703",
        "c152157d-3ee6-4361-840b-31e120f45ecb",
        "7ef09054-c50e-4327-a7c5-95f22df0bd19",
        "458a7b6e-2881-467e-a225-3aa9d32e0e61",
        "f457897a-1d02-43c2-9edb-7d69d6a4a634",
        "ba752521-924f-4a5a-9507-013aa517696d"
    ]
    session_list = [
        '1d0b2814-b100-4095-8b41-1cc8843a6703'
    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
