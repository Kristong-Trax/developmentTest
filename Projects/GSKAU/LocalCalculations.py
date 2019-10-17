
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKAU.SceneKpis.SceneCalculations import SceneCalculations
from collections import OrderedDict

if __name__ == '__main__':
    LoggerInitializer.init('gskau-sand calculations')
    Config.init()
    project_name = 'gskau-sand'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ("BAE5D1D0-B341-45AE-BA99-889B611266C8", [23610]),  # no targets match [multi bay]
        ("82CFBFC1-BDD9-4CB8-8B39-87C11B6FF62E", [23613]),  # multi bay
        ("060C5E25-2B5B-4F95-BA99-50325E866407", [23598]),  # multi bay
        ("749AD6BB-54DA-4727-9852-A6D503BECB96", [23595]),  # multi bay
        ("26595CE1-1F43-4F50-9918-45EB1B34DDCE", [23604]),  # multi bay
        ("286DACD0-A5AA-42AD-BC7E-6BFC4A0135B3", [23607]),  # multi bay
        ("103CDB86-69BF-4B4E-9B34-E32B0FA8B235", [23601]),
        ("59EC0A21-500E-4E78-88AC-5B759FB39731", [23586]),
        ("9BF79D4B-A15F-4EF3-B687-499ACC51669B", [23589]),
        ("8F9D4831-2DFD-4F36-B6A5-BA3273CDEBBB", [23592]),
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
            data_provider.load_scene_data(session, e_scene)
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
