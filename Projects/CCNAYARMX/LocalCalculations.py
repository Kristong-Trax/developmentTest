from collections import OrderedDict
import csv
import os

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCNAYARMX.Calculations import Calculations

DATA_PATH = os.path.join(os.path.dirname(__file__), 'Data')


def read_sessions(filename):
    """
    Reads CSV with `filename` from Data folder into memory.

    :param filename: Name of CSV file to read.
    :return: List of sessions.
    """

    with open(os.path.join(DATA_PATH, filename+'.csv'), 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        sessions = [row[0] for row in csv_reader]

    return sessions


def run_sessions(session_list):
    """
    Iterates through `session_list` running calculations on then recording each session.

    :param session_list: List of sessions to run calculations on.
    """

    for session in session_list:
        if session in completed_sessions:
            continue
        print("==================== {} ====================".format(session))
        data_provider.load_session_data(session_uid=session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        # record_session(session)


def record_session(session):
    """
    Appends `session` to the end of 'tested_sessions.csv'
    and removes it from 'test_sessions.csv'.

    :param session: most recently run session to add to 'tested_sessions.csv'.
    """

    completed_sessions.append(session)

    # add session to 'tested_sessions.csv'
    with open(os.path.join(DATA_PATH, 'tested_sessions.csv'), 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        tested_sessions = OrderedDict.fromkeys([row[0] for row in csv_reader]).keys()

    try:
        tested_sessions.remove(session)
    except ValueError:
        pass
    tested_sessions.append(session)

    with open(os.path.join(DATA_PATH, 'tested_sessions.csv'), 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows([[session] for session in tested_sessions])

    # remove session from 'test_sessions.csv'
    with open(os.path.join(DATA_PATH, 'test_sessions.csv'), 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        test_sessions = {row[0] for row in csv_reader}

    if len(test_sessions) > 0:
        test_sessions.discard(session)

        with open(os.path.join(DATA_PATH, 'test_sessions.csv'), 'wb') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows([[session] for session in test_sessions])


if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx calculations')
    Config.init()
    data_provider = KEngineDataProvider(project_name='ccnayarmx')

    check_sessions = [
        # 'd5613cbc-6f10-46a2-9e99-acb6c8c21edb',
        # 'cf12065b-e383-4009-a1e8-c8d9ced9bbf9',
        # 'b974cb3b-a609-4610-ab14-ae94163d0c76',
        'f8f413dc-7a9c-47d6-813b-dadd7f834a5f',
        # 'a25897ed-412a-4c13-a65a-7e89a143f08f',
        # 'd64cbf6a-c1eb-4c10-a880-668617dd38ac'
        # '77acfbe0-347e-4f23-bab6-f93781515b31',



        # 'aa743d1f-07af-494d-83f8-daaa508a8b01',

        # 'e4aa24a5-d13e-4b6a-8941-0a195d589070',  # error
        # '5f20ba65-63ae-4057-a8b0-3491accf7869',  # fine
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69',
        # '0eda0210-b4ed-461c-8b32-09bfddf0cab8',
        # '1c7303e6-96bc-4360-822f-e00886701a1b',
        # '9ba32139-c2bd-4d36-96b3-6268628960ee',
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69',
        # '69b028e2-5119-4c11-a846-94ca29adede4',  # fine
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69',
        # '725524e8-2e9b-4c42-ace1-b2120d987f9e',
        # '90004fbd-58dd-418f-a359-f2605134291c',
        # '677d0628-9566-4760-8702-821882f74665',
        # 'f6f086d8-b269-4f2d-8229-144af3b1edf8',
        # '7e117743-0448-447b-9ad3-7c895ca8a0b7',
        # '543f7eff-309b-419c-8f39-931cc5cbcba6',
        # '199eda17-4fbb-4c23-b02d-8f3f47e079d9',
    ]

    completed_sessions = []

    run_sessions(check_sessions)
    # run_sessions(read_sessions('test_sessions'))
    # run_sessions(read_sessions('tested_sessions'))
