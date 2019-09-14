from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU calculations')
    project_name = 'marsru-prod'
    session_uids = [
        '8e71f5b3-0a46-414d-a14b-80a2dbc510ea',
        '92a437fc-f7bf-4611-9dfc-d5517b48c99a',
        'a8cc0ef6-e96c-4181-8187-d0e63480d656',
        'bc8a6ed0-5406-434c-b0de-87e107265796',
        'b21f3e2b-bd52-4e53-bcb6-134f4b3a8bed',
        # 'f07c8388-fa93-481f-84d3-0ed1b4ba4def'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()
