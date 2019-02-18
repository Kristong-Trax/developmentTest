
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJUKTRIAL_SAND.Calculations import JNJUKTRIAL_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjuk calculations')
    Config.init()
    project_name = 'jnjuktrial-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '8d4af1e5-6c22-4a47-8ad9-e309dba85391'
        # 'faa35677-1473-47cd-a5fb-ca1c0e979d16'
        # 'b741be7a-8cf4-4cb5-8413-097544396535',
        #         '21bdd793-b6bd-4d51-8706-30d22f914513',
        #         'fac30e59-0fe7-4133-8678-8801c5796df5',
        #         '19a87af2-a24c-4f41-b4a0-93d62b354190',
        #         '234569b1-2ac9-4fcb-9dbf-b0742aadb8b8',
        #         '61f76f46-0290-47c8-bc29-ef7477010caa',
                ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJUKTRIAL_SANDCalculations(data_provider, output).run_project_calculations()
