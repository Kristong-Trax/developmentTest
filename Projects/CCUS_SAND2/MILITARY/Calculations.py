from __future__ import division

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCUS_SAND2.MILITARY.KPIGenerator import MilitaryGenerator

__author__ = "trevaris"

test_sessions = [
    # '7AD7D0DE-01D8-432A-AD63-B31AAD48767B',
    # '1e55cbe5-027d-450a-92c6-bcbf31661dd7',
    # '59e685be-0734-4b6f-8097-fcd09ef777b9',
    '9965dff6-a5af-4acf-8664-7a30cc6b6abd',
    'b84dc417-ce08-4328-b85b-c84a515474c1',
    '9807b657-1cec-4d5a-82bd-83ec89b0bd8b',  # no products
    'cf54d865-f0a6-4f04-9b66-7c579e1ca8e3',  # no products
    '8b9bed83-1ce8-4e68-b20e-0711d1263238',  # no products
    '9d364d60-edb4-430e-8f37-0246c880e21b',  # no products
    '841cd391-d323-481d-8fae-40bc32276195',  # no products?
]


def run_sessions(sessions):
    for i, session in enumerate(sessions, 1):
        run_session(session)
        complete = round(i / len(sessions) * 100, 2)
        print("Completed {}% of sessions".format(round(complete)))


def run_session(session_uid):
    print("==================== {} ====================".format(session_uid))
    data_provider.load_session_data(session_uid)
    output = Output()
    MilitaryGenerator(data_provider, output).main_function()


if __name__ == '__main__':
    LoggerInitializer.init('CCUS/Military Calculations')
    Config.init()
    project_name = 'ccus-sand2'
    data_provider = KEngineDataProvider(project_name)
    run_sessions(test_sessions)
