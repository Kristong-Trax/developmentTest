import csv
import os
import random

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Projects.RINIELSENUS.SceneKpis.SceneCalculations import SceneCalculations
from Projects.RINIELSENUS.TYSON.KPIGenerator import TysonGenerator

PROJECT_PATH = os.path.dirname(__file__)
RETEST_SESSIONS = 10
NEW_SESSIONS = 5


class TysonCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        try:
            TysonGenerator(self.data_provider, self.output).main_function()
        except Exception:
            Log.error("Tyson KPIs not calculated.")
        self.timer.stop('KPIGenerator.run_project_calculations')


def run_sessions(sessions):
    for i, session in enumerate(sessions, start=1):
        print("======================================== {} ========================================".format(session))
        data_provider.load_session_data(session)
        output = Output()
        TysonCalculations(data_provider, output).run_project_calculations()
        print("Completed {}% of local test sessions".format(round(float(i)/len(sessions)*100, 2)))


def run_tested_sessions(num_sessions=True):
    """
    Run random selection of sessions previously run
    or all previously run sessions if `num_sessions` is True.

    :param num_sessions: Number of previously run sessions to run.
    """

    with open(os.path.join(PROJECT_PATH, "Utils", "tested_sessions.csv"), 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        tested_sessions = [row[0] for row in csv_reader if row[0] not in sessions]

    if num_sessions is True:
        retest_sessions = tested_sessions
    else:
        retest_sessions = random.sample(tested_sessions, num_sessions)

    run_sessions(retest_sessions)

    return tested_sessions


def run_random_sessions(num_session):
    """
    Run random selection of relevant sessions.

    :param num_session: Number of new sessions to run.
    "return: The list of chosen sessions.
    """

    with open(os.path.join(PROJECT_PATH, "Utils", "tyson_relevant_sessions.csv"), 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        relevant_sessions = [row[0] for row in csv_reader if row[0] not in sessions + retested_sessions]
    random_sessions = random.sample(relevant_sessions, num_session)
    run_sessions(random_sessions)

    return random_sessions


def write_tested_session(test_sessions, tested_sessions, sample_sessions):
    """
    Store all calculated sessions.
    """

    tested_sessions = set(tested_sessions + test_sessions + sample_sessions)
    with open(os.path.join(PROJECT_PATH, "Utils", "tested_sessions.csv"), 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows([[session] for session in tested_sessions])


if __name__ == '__main__':
    LoggerInitializer.init('Tyson calculations')
    Config.init()
    project_name = 'rinielsenus'
    data_provider = KEngineDataProvider(project_name)

    # run specific sessions
    sessions = [
        # '70fcb7e9-72bd-4799-afea-4d018e142c5b',
        # '3c0abb33-5f80-447c-9d94-80d399930cf2',
        # '79eb1a47-25b3-4aa6-9266-221d20b2e553',
        # 'ea434336-4144-4e82-ae34-2e8307c1004f',
        # 'daf7fd47-fadb-455e-acd6-9926ed71ee73',
        # 'b9cdc474-ccae-424b-a12c-c0c0a1195af1'
    ]

    run_sessions(sessions)
    retested_sessions = run_tested_sessions(RETEST_SESSIONS)
    sample_sessions = run_random_sessions(NEW_SESSIONS)
    write_tested_session(sessions, retested_sessions, sample_sessions)
    print("Done")

# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)

# if __name__ == '__main__':
#     LoggerInitializer.init('Tyson calculations')
#     Config.init()
#     project_name = 'rinielsenus'
#
#     sessions = [
#         '23ad27c5-888c-4f84-a4ee-a8f959b563ff',
#         '5d85be01-83b1-49bf-81bc-19ebe720a61d',
#         'ec3fdc9f-235f-4b3f-92ee-f44bf51fc7e4',
#         '868a8683-f99a-4906-b175-590a082e9c7c'
#     ]
#
#     for session in sessions:
#         print('\n~~~~~~~~~~~~~~~~~~~~~~~ {} ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#
#         scif = data_provider['scene_item_facts']
#         scenes = scif['scene_uid'].unique().tolist()
#
#         scenes = ['9ff5527d-b3ad-4ba6-b8e9-0c5b31e47819',
#                   '0bae2465-81a8-41cd-a20c-66eb92917759',
#                   '8c2573ae-456e-4825-ac83-0ecb828b87dd',
#                   'd076a8ac-525e-4a14-9f35-7481ab859b9c'
#                   ]
#
#         for scene in scenes:
#             print('\n-------------------------------- {} ----------------------------'.format(scene))
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene_uid=scene)
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#
#         # output = Output()
#         # Calculations(data_provider, output).run_project_calculations()
