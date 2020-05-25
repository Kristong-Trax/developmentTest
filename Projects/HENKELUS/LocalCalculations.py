
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.HENKELUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('henkelus calculations')
    Config.init()
    project_name = 'henkelus'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        # "be04d3a9-e4ee-4ea8-b7c7-4ed2c39f4548",
        #             '23388796-e888-497b-bcbe-efe3d95f286d',
                    'a8e1b893-e0cc-475b-8537-f459bdc21f4d',
                    '7128148f-08e7-4c9f-9373-1c1a21be3e96',
                    # 'c59fb6c0-8727-44d6-af62-98c195baa74a',
                    # 'dc6b9147-70ba-4f60-bb0d-9231dab2fd1f'

                    # 'fd6ee83a-a166-4957-98ac-c28e698413ac',
                    # '59cb4616-8d1c-46c1-9805-99f148e4ffc4',
                    # '16e7e121-ae67-4e50-9e14-159f9dcb4d73',
                    # 'fc4260f6-2e84-41d8-aba7-26bef28f2072',
                    # '9f14f081-bb7c-4623-ba93-d8571c4698fc',
                    # 'd4b24e75-35fa-4c48-9fb7-6252f5ad5df3',
                    # 'e5cac33d-d1c2-4c37-8029-30a60e04908f',
                    # 'e75f5f09-9861-45aa-b6bd-ccd89b20b72d',
                    # '5e27af41-b607-48ad-81f1-6585a3083253',
                    # '5567e39e-a2ec-4708-810c-6caa8057c522',
                    # 'a5e3d3ff-38d7-4dab-8b05-5b1be592d098',
                    # 'ad5c3190-9b77-478b-8ed8-12ba5977dc28',
                    # '107eda78-dd05-471c-9625-132f2e893e97',
                    # '039125d6-6bfb-42ed-b6af-97637b1a7e71',
                    # 'bab01ed2-d444-4958-ab4a-608bbf8088d1',
                    ]
    for session in session_list:
        print(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
