
import os
import shutil

from KPIUtils_v2.Utils.Decorators.Decorators import log_task
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging import Metrics
from Trax.Utils.Logging.Logger import Log
import stat
from Trax.Apps.Services.PSMonitor.Server import PsMonitor

from DevloperTools.ProjectCreator.Consts import MAIN_FILE_NAME, MAIN_FILE, LOCAL_CALCULATIONS_FILE_NAME, LOCAL_FILE, \
    GENERATOR_FILE_NAME, GENERATOR, TOOL_BOX_FILE_NAME, TOOL_BOX, PROFILING_SCRIPT_NAME, PROFILING_SCRIPT, \
    GEN_DEPENDENCY_SCRIPT, DEPENDENCIES_SCRIPT_NAME, TESTS_SCRIPT_NAME, TEST_SCRIPT, SCENE_TOOLBOX_FILE_NAME, \
    SCENE_TOOLBOX_SCRIPT, SCENE_GENERATOR_SCRIPT, SCENE_GENERATOR_FILE_NAME, SCENE_CALCULATIONS_FILE_NAME, \
    SCENE_CALCULATIONS_SCRIPT

__author__ = 'yoava'


class CreateKPIProject:

    # @log_task(monitor_screen='Task', monitor_object='New Project')
    def __init__(self, project_name, calculate_by_scene=False):
        self.project = project_name.lower().replace('_', '-')
        self.project_capital = self.project.upper().replace('-', '_')
        self.project_short = self.project_capital.split('_')[0]
        self.author = os.environ.get('USER', '')
        self.project_path = self.get_project_path()
        self.calculate_by_scene = calculate_by_scene
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

    def create_new_project(self):
        monitor = PsMonitor()
        monitor.send_to_grafana(monitor_screen='TASKTASK', monitor_object='NEW PROJECT', project=self.project, value=1)

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
                           'generator_class_name': 'Generator',
                           'scene_generator_class_name': 'SceneGenerator',
                           'tool_box_file_name': TOOL_BOX_FILE_NAME,
                           'scene_tool_box_file_name': SCENE_TOOLBOX_FILE_NAME,
                           'tool_box_class_name': '{}ToolBox'.format(self.project_short),
                           'scene_tool_box_class_name': '{}SceneToolBox'.format(self.project_short),
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
            files_to_create[''].append((SCENE_CALCULATIONS_FILE_NAME, SCENE_CALCULATIONS_SCRIPT))
        return files_to_create


if __name__ == '__main__':
    Config.init('')
    LoggerInitializer.init('Creating new project')
    project = 'test1'
    Log.info("project name : " + project)
    new = CreateKPIProject(project)
    new.create_new_project()
    Log.info("project : " + project + " was created successfully")
