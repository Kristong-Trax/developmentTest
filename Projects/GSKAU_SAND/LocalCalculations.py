
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Projects.GSKAU_SAND.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output


if __name__ == '__main__':
    LoggerInitializer.init('gskau-sand calculations')
    Config.init()
    project_name = 'gskau-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'FF84FF47-912C-4269-BE6A-F3C61FAF72C3',  # '2019-07-09'
        '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',  # '2019-07-09'
        '9DC66118-F981-4D01-A6DD-1E181FA05507',  # '2019-07-01'
        '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',  # '2019-07-10'  by store number
        '6520B138-780D-4AD1-95CF-8DA1727C4580',  # '2019-07-11'
        '9181C056-0172-4EA6-A6B6-1B8CB3FB9B30',  # '2019-07-18'
        'E0BE9853-B6C8-4B36-919C-5293BF52EF5B',  # '2019-08-05'
    ]
    for sess in sessions:
        Log.info("Running for session: {}".format(sess))
        data_provider.load_session_data(sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
