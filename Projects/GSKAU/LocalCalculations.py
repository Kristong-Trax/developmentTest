
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKAU.SceneKpis.SceneCalculations import SceneCalculations
from collections import OrderedDict

if __name__ == '__main__':
    LoggerInitializer.init('gskau calculations')
    Config.init()
    project_name = 'gskau'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ("55F4ED38-15FE-4C93-B3EC-BC036E764274", [175323]),
        ("9bec2339-594c-4a98-be5b-a159ef6ddb72", [175029]),
        ("a5a22623-1e7c-46ee-97c7-f2a198a057e2", [174626, 174640, 174960])
    ])
    # session_scene_map = OrderedDict([
    #     ('002FF630-6FEB-43B3-8916-7B85D864B903', [4154, 4259, 4359, 4361]),  # only 4359
    #     ('40F1D7D1-93CC-409D-A3D7-2BCC5175476A', [6933, 6925, 6922]),  # wont work 6925, 6922
    #     ('6944F6F5-79D7-43FB-BE32-E3FAE237FA63', [10566]),  # this!
    #     ('54DA2146-C09A-4C29-B77B-2A2B38E66BD0', [15743]),
    #     ('00703094-4889-441D-A9AC-DAF0D32EFC45', [18308]),  # this is ok
    #     ('7275C65F-F479-44EA-A431-38169655C201', [18137]),
    #     # these wont work
    #     ('04AEDCB1-AFB5-441A-87A9-027F406F8125', [16705]),  # 'Oral Main Shelf' -- wont calc
    #     ('43775D5F-F9BA-46C4-B349-834F0A5DF8E2', [18247]),  # wont work cuz its IND Grocery and T
    #     ('1A77E364-551D-4036-8248-87C4000A4C28', [3981]),
    #     ('22F9B0F8-9D8C-41C4-9D1C-5838F4FA9DFD', [22074]),
    # ]
    # )
    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider = KEngineDataProvider(project_name)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            data_provider.load_scene_data(session, scene_id=e_scene)
            SceneCalculations(data_provider).calculate_kpis()
#     sessions = [
#         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B',
#         '6520B138-780D-4AD1-95CF-8DA1727C4580',
#         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',
#         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',
#         '9DC66118-F981-4D01-A6DD-1E181FA05507',
#         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B'
#     ]
#     # RUN FOR Session level KPIs
#     for sess in sessions:
#         Log.info("[Session level] Running for session: {}".format(sess))
#         data_provider.load_session_data(sess)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
