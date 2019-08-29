
import os
import shutil

from KPIUtils_v2.Utils.Decorators.Decorators import log_task
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
import stat
from DevloperTools.ProjectCreator.Consts import Const, SCENE_TOOLBOX_SCRIPT, SCENE_GENERATOR_SCRIPT, TEST_SCRIPT, \
    PLANOGRAM_COMPLIANCE_CALCULATIONS_SCRIPT, PLANOGRAM_FINDER_CALCULATIONS_SCRIPT, PLANOGRAM_CALCULATIONS_SCRIPT, \
    PLANOGRAM_GENERATOR_SCRIPT, PLANOGRAM_TOOLBOX_SCRIPT, LIVE_SCENE_CALCULATIONS_SCRIPT, LIVE_SCENE_TOOLBOX_SCRIPT, \
    LIVE_SESSION_TOOLBOX_SCRIPT, LIVE_SESSION_CALCULATIONS_SCRIPT, LIVE_SESSION_GENERATOR_SCRIPT, SCENE_CALCULATIONS, \
    LIVE_SCENE_GENERATOR_SCRIPT, CALCULATIONS, LOCAL_CALCS, GENERATOR, TOOL_BOX, PROFILING_SCRIPT, \
    GEN_DEPENDENCY_SCRIPT, LOCAL_CALCS_WITH_SCENES, LOCAL_CONSTS, get_project_name_and_directory_name

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
        self.project, self.project_capital = get_project_name_and_directory_name(project_name)
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

    def get_formatting_dict(self):
        formatting_dict = {
            'author': self.author, 'project': self.project, 'project_capital': self.project_capital,
            'generator_file_name': Const.GENERATOR_FILE_NAME,
            'scene_generator_file_name': Const.SCENE_GENERATOR_FILE_NAME,
            'planogram_generator_file_name': Const.PLANOGRAM_GENERATOR_FILE_NAME,
            'live_scene_generator_file_name': Const.LIVE_SCENE_GENERATOR_FILE_NAME,
            'live_scene_generator_class_name': Const.LIVE_SCENE_GENERATOR_CLASS_NAME,
            'live_session_generator_file_name': Const.LIVE_SESSION_GENERATOR_FILE_NAME,
            'live_session_generator_class_name': Const.LIVE_SESSION_GENERATOR_CLASS_NAME,
            'generator_class_name': Const.GENERATOR_CLASS_NAME,
            'scene_generator_class_name': Const.SCENE_GENERATOR_CLASS_NAME,
            'planogram_generator_class_name': Const.PLANOGRAM_GENERATOR_CLASS_NAME,
            'tool_box_file_name': Const.TOOL_BOX_FILE_NAME,
            'scene_tool_box_file_name': Const.SCENE_TOOLBOX_FILE_NAME,
            'planogram_tool_box_file_name': Const.PLANOGRAM_TOOLBOX_FILE_NAME,
            'live_scene_tool_box_file_name': Const.LIVE_SCENE_TOOLBOX_FILE_NAME,
            'live_session_tool_box_file_name': Const.LIVE_SESSION_TOOLBOX_FILE_NAME,
            'tool_box_class_name': Const.TOOL_BOX_CLASS_NAME,
            'scene_tool_box_class_name': Const.SCENE_TOOL_BOX_CLASS_NAME,
            'planogram_tool_box_class_name': Const.PLANOGRAM_TOOL_BOX_CLASS_NAME,
            'live_scene_tool_box_class_name': Const.LIVE_SCENE_TOOL_BOX_CLASS_NAME,
            'live_session_tool_box_class_name': Const.LIVE_SESSION_TOOL_BOX_CLASS_NAME,
            'main_class_name': Const.MAIN_CLASS_NAME, 'main_file_name': Const.MAIN_FILE_NAME}
        return formatting_dict

    def get_files_to_create(self):
        if self.calculate_by_scene:
            local_calc_script = LOCAL_CALCS_WITH_SCENES
        else:
            local_calc_script = LOCAL_CALCS
        files_to_create = {'': [(Const.MAIN_FILE_NAME, CALCULATIONS),
                                (Const.LOCAL_CALCULATIONS_FILE_NAME, local_calc_script),
                                (Const.GENERATOR_FILE_NAME, GENERATOR)],
                           'Utils': [(Const.TOOL_BOX_FILE_NAME, TOOL_BOX)],
                           'Profiling': [(Const.PROFILING_SCRIPT_NAME, PROFILING_SCRIPT),
                                         (Const.DEPENDENCIES_SCRIPT_NAME, GEN_DEPENDENCY_SCRIPT)],
                           'Tests': [(Const.TESTS_SCRIPT_NAME + '_{}'.format(self.project), TEST_SCRIPT)],
                           'Data': [(Const.LOCAL_CONSTS_FILE_NAME, LOCAL_CONSTS)]}
        if self.calculate_by_scene:
            files_to_create['Utils'].append((Const.SCENE_TOOLBOX_FILE_NAME, SCENE_TOOLBOX_SCRIPT))
            files_to_create[''].append((Const.SCENE_GENERATOR_FILE_NAME, SCENE_GENERATOR_SCRIPT))
            files_to_create['SceneKpis'] = [(Const.SCENE_CALCULATIONS_FILE_NAME, SCENE_CALCULATIONS)]
        if self.calculate_by_planogram:
            files_to_create['Utils'].append((Const.PLANOGRAM_TOOLBOX_FILE_NAME, PLANOGRAM_TOOLBOX_SCRIPT))
            files_to_create[''].append((Const.PLANOGRAM_GENERATOR_FILE_NAME, PLANOGRAM_GENERATOR_SCRIPT))
            files_to_create['PlanogramKpis'] = [(Const.PLANOGRAM_CALCULATIONS_FILE_NAME, PLANOGRAM_CALCULATIONS_SCRIPT)]
        if self.planogram_compliance_calculation:
            files_to_create['PlanogramCompliance'] = [(Const.PLANOGRAM_COMPLIANCE_CALCULATIONS_FILE_NAME,
                                                      PLANOGRAM_COMPLIANCE_CALCULATIONS_SCRIPT)]
            files_to_create['PlanogramFinder'] = [(Const.PLANOGRAM_FINDER_CALCULATIONS_FILE_NAME,
                                                  PLANOGRAM_FINDER_CALCULATIONS_SCRIPT)]
        if self.trax_live_scene:
            files_to_create['Utils'].append((Const.LIVE_SCENE_TOOLBOX_FILE_NAME, LIVE_SCENE_TOOLBOX_SCRIPT))
            files_to_create[''].append((Const.LIVE_SCENE_GENERATOR_FILE_NAME, LIVE_SCENE_GENERATOR_SCRIPT))
            files_to_create['LiveSceneKpis'] = [(Const.LIVE_SCENE_CALCULATIONS_FILE_NAME, LIVE_SCENE_CALCULATIONS_SCRIPT)]
        if self.trax_live_session:
            files_to_create['Utils'].append((Const.LIVE_SESSION_TOOLBOX_FILE_NAME, LIVE_SESSION_TOOLBOX_SCRIPT))
            files_to_create[''].append((Const.LIVE_SESSION_GENERATOR_FILE_NAME, LIVE_SESSION_GENERATOR_SCRIPT))
            files_to_create['LiveSessionKpis'] = [(Const.LIVE_SESSION_CALCULATIONS_FILE_NAME, LIVE_SESSION_CALCULATIONS_SCRIPT)]

        return files_to_create


if __name__ == '__main__':
    try:
        LoggerInitializer.init('new_project')
        Config.init(app_name='new_project_new')
        project = 'avi-sand'
        Log.info("project name : " + project)
        new = CreateKPIProject(project, calculate_by_scene=True)
        new.create_new_project()
        Log.info('project {} was created successfully'.format(project))
    except Exception as e:  
        Log.warning(str(e))
