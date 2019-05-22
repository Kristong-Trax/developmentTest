
import os
import shutil

from KPIUtils_v2.Utils.Decorators.Decorators import log_task
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
import stat

from DevloperTools.ProjectCreator.Consts import MAIN_FILE_NAME, MAIN_FILE, LOCAL_CALCULATIONS_FILE_NAME, LOCAL_FILE, \
    GENERATOR_FILE_NAME, GENERATOR, TOOL_BOX_FILE_NAME, TOOL_BOX, PROFILING_SCRIPT_NAME, PROFILING_SCRIPT, \
    GEN_DEPENDENCY_SCRIPT, DEPENDENCIES_SCRIPT_NAME, TESTS_SCRIPT_NAME, TEST_SCRIPT, SCENE_TOOLBOX_FILE_NAME, \
    SCENE_TOOLBOX_SCRIPT, SCENE_GENERATOR_SCRIPT, SCENE_GENERATOR_FILE_NAME, SCENE_CALCULATIONS_FILE_NAME, \
    SCENE_CALCULATIONS_SCRIPT, PLANOGRAM_COMPLIANCE_CALCULATIONS_FILE_NAME, PLANOGRAM_COMPLIANCE_CALCULATIONS_SCRIPT, \
    PLANOGRAM_FINDER_CALCULATIONS_FILE_NAME, PLANOGRAM_FINDER_CALCULATIONS_SCRIPT, PLANOGRAM_GENERATOR_FILE_NAME, \
    PLANOGRAM_CALCULATIONS_SCRIPT, PLANOGRAM_GENERATOR_SCRIPT, PLANOGRAM_CALCULATIONS_FILE_NAME, \
    PLANOGRAM_TOOLBOX_FILE_NAME, PLANOGRAM_TOOLBOX_SCRIPT, LIVE_SCENE_GENERATOR_FILE_NAME, \
    LIVE_SCENE_TOOLBOX_FILE_NAME, LIVE_SCENE_CALCULATIONS_FILE_NAME, LIVE_SCENE_CALCULATIONS_SCRIPT, \
    LIVE_SCENE_GENERATOR_SCRIPT, LIVE_SCENE_TOOLBOX_SCRIPT, LIVE_SCENE_GENERATOR_CLASS_NAME, \
    PLANOGRAM_GENERATOR_CLASS_NAME, SCENE_GENERATOR_CLASS_NAME, LIVE_SESSION_GENERATOR_FILE_NAME, \
    LIVE_SESSION_GENERATOR_CLASS_NAME, LIVE_SESSION_TOOLBOX_FILE_NAME, LIVE_SESSION_TOOLBOX_SCRIPT, \
    LIVE_SESSION_GENERATOR_SCRIPT, LIVE_SESSION_CALCULATIONS_FILE_NAME, LIVE_SESSION_CALCULATIONS_SCRIPT

__author__ = 'yoava'


class CreateKPIProject:

    """
    this class creates new KPI project
    you can set in the constructor if you want to add to the normal project calculations by scene, by plangoram ,
    or by planogram compliance by setting the parameters to True
    all params are False by default
    """
    def __init__(self, project_name, calculate_by_scene=False, calculate_by_planogram=False,
                 planogram_compliance=False, trax_live_scene=False, trax_live_session=False):
        self.project = project_name.lower().replace('_', '-')
        self.project_capital = self.project.upper().replace('-', '_')
        self.project_short = self.project_capital.split('_')[0]
        self.author = os.environ.get('USER', '')
        self.project_path = self.get_project_path()
        self.calculate_by_scene = calculate_by_scene
        self.calculate_by_planogram = calculate_by_planogram
        self.planogram_compliance_calculation = planogram_compliance
        self.trax_live_scene = trax_live_scene
        self.trax_live_session = trax_live_session
        self.create_project_directory()

    def get_project_path(self):
        path_to_list = os.path.abspath(__file__).split('/')

        path = "{0}/{1}/{2}/{3}/{4}/Projects/{5}/".format(path_to_list[0], path_to_list[1], path_to_list[2],
                                                          path_to_list[3], path_to_list[4], self.project_capital)
        return path

    def create_project_directory(self):
        if os.path.exists(self.project_path):
            shutil.rmtree(self.project_path)
        os.mkdir(self.project_path)
        with open(self.project_path + '__init__.py', 'wb') as f:
            f.write('')

    @log_task(action='new_project', message='Project created successfully', environment='prod')
    def create_new_project(self):
        files_to_create = self.get_files_to_create()

        formatting_dict = self.get_formatting_dict()

        for directory in files_to_create.keys():
            if directory:
                directory_path = self.project_path + directory + '/'
                os.mkdir(directory_path)
                with open(directory_path + '__init__.py', 'wb') as f:
                    f.write('')
            else:
                directory_path = self.project_path
            for file_name, file_content in files_to_create[directory]:
                if directory == 'Profiling':
                    with open(directory_path + file_name + '.sh', 'wb') as f:
                        f.write(file_content)
                        st = os.stat(os.path.join(directory_path, file_name + '.sh'))
                        os.chmod(os.path.join(directory_path, file_name + '.sh'), st.st_mode | stat.S_IEXEC)
                else:
                    with open(directory_path + file_name + '.py', 'wb') as f:
                        f.write(file_content % formatting_dict)

        data_directory = os.path.join(self.project_path, 'Data')
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)

    def get_formatting_dict(self):
        formatting_dict = {'author': self.author,
                           'project': self.project,
                           'project_capital': self.project_capital,
                           'generator_file_name': GENERATOR_FILE_NAME,
                           'scene_generator_file_name': SCENE_GENERATOR_FILE_NAME,
                           'planogram_generator_file_name': PLANOGRAM_GENERATOR_FILE_NAME,
                           'live_scene_generator_file_name': LIVE_SCENE_GENERATOR_FILE_NAME,
                           'live_scene_generator_class_name': LIVE_SCENE_GENERATOR_CLASS_NAME,
                           'live_session_generator_file_name': LIVE_SESSION_GENERATOR_FILE_NAME,
                           'live_session_generator_class_name': LIVE_SESSION_GENERATOR_CLASS_NAME,
                           'generator_class_name': 'Generator',
                           'scene_generator_class_name': SCENE_GENERATOR_CLASS_NAME,
                           'planogram_generator_class_name': PLANOGRAM_GENERATOR_CLASS_NAME,
                           'tool_box_file_name': TOOL_BOX_FILE_NAME,
                           'scene_tool_box_file_name': SCENE_TOOLBOX_FILE_NAME,
                           'planogram_tool_box_file_name': PLANOGRAM_TOOLBOX_FILE_NAME,
                           'live_scene_tool_box_file_name': LIVE_SCENE_TOOLBOX_FILE_NAME,
                           'live_session_tool_box_file_name': LIVE_SESSION_TOOLBOX_FILE_NAME,
                           'tool_box_class_name': '{}ToolBox'.format(self.project_short),
                           'scene_tool_box_class_name': '{}SceneToolBox'.format(self.project_short),
                           'planogram_tool_box_class_name': '{}PlanogramToolBox'.format(self.project_short),
                           'live_scene_tool_box_class_name': '{}LiveSceneToolBox'.format(self.project_short),
                           'live_session_tool_box_class_name': '{}LiveSessionToolBox'.format(self.project_short),
                           'main_file_name': MAIN_FILE_NAME,
                           'main_class_name': '{}Calculations'.format(self.project_short)
                           }
        return formatting_dict

    def get_files_to_create(self):
        files_to_create = {'': [(MAIN_FILE_NAME, MAIN_FILE),
                                (LOCAL_CALCULATIONS_FILE_NAME, LOCAL_FILE),
                                (GENERATOR_FILE_NAME, GENERATOR)],
                           'Utils': [(TOOL_BOX_FILE_NAME, TOOL_BOX),
                                     ],
                           'Profiling': [(PROFILING_SCRIPT_NAME, PROFILING_SCRIPT),
                                         (DEPENDENCIES_SCRIPT_NAME, GEN_DEPENDENCY_SCRIPT)],
                           'Tests': [(TESTS_SCRIPT_NAME + '_{}'.format(self.project), TEST_SCRIPT)]}
        if self.calculate_by_scene:
            files_to_create['Utils'].append((SCENE_TOOLBOX_FILE_NAME, SCENE_TOOLBOX_SCRIPT))
            files_to_create[''].append((SCENE_GENERATOR_FILE_NAME, SCENE_GENERATOR_SCRIPT))
            files_to_create['SceneKpis'] = [(SCENE_CALCULATIONS_FILE_NAME, SCENE_CALCULATIONS_SCRIPT)]
        if self.calculate_by_planogram:
            files_to_create['Utils'].append((PLANOGRAM_TOOLBOX_FILE_NAME, PLANOGRAM_TOOLBOX_SCRIPT))
            files_to_create[''].append((PLANOGRAM_GENERATOR_FILE_NAME, PLANOGRAM_GENERATOR_SCRIPT))
            files_to_create['PlanogramKpis'] = [(PLANOGRAM_CALCULATIONS_FILE_NAME, PLANOGRAM_CALCULATIONS_SCRIPT)]
        if self.planogram_compliance_calculation:
            files_to_create['PlanogramCompliance'] = [(PLANOGRAM_COMPLIANCE_CALCULATIONS_FILE_NAME,
                                                      PLANOGRAM_COMPLIANCE_CALCULATIONS_SCRIPT)]
            files_to_create['PlanogramFinder'] = [(PLANOGRAM_FINDER_CALCULATIONS_FILE_NAME,
                                                  PLANOGRAM_FINDER_CALCULATIONS_SCRIPT)]
        if self.trax_live_scene:
            files_to_create['Utils'].append((LIVE_SCENE_TOOLBOX_FILE_NAME, LIVE_SCENE_TOOLBOX_SCRIPT))
            files_to_create[''].append((LIVE_SCENE_GENERATOR_FILE_NAME, LIVE_SCENE_GENERATOR_SCRIPT))
            files_to_create['LiveSceneKpis'] = [(LIVE_SCENE_CALCULATIONS_FILE_NAME, LIVE_SCENE_CALCULATIONS_SCRIPT)]
        if self.trax_live_session:
            files_to_create['Utils'].append((LIVE_SESSION_TOOLBOX_FILE_NAME, LIVE_SESSION_TOOLBOX_SCRIPT))
            files_to_create[''].append((LIVE_SESSION_GENERATOR_FILE_NAME, LIVE_SESSION_GENERATOR_SCRIPT))
            files_to_create['LiveSessionKpis'] = [(LIVE_SESSION_CALCULATIONS_FILE_NAME, LIVE_SESSION_CALCULATIONS_SCRIPT)]

        return files_to_create


if __name__ == '__main__':
    try:
        LoggerInitializer.init('new_project')
        Config.init(app_name='new_project_new')
        project = 'gskau'
        Log.info("project name : " + project)
        new = CreateKPIProject(project)
        new.create_new_project()
        Log.info('project {} was created successfully'.format(project))
    except Exception as e:  
        Log.warning(str(e))