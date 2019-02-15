from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru-prod'
    session_uids = [
        # 'fe1970d0-240b-4043-86d6-3edc448818f1',
        # 'b482eda7-b892-4493-bdda-a8f29931eb09',
        '03ea1b66-6882-4b2f-bbdc-8e199bce4831',
        'fc7faf83-4ab5-4aa0-96cb-47888f9d8288',
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()

