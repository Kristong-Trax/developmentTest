import imp
import inspect
import os

PYTHON_EXTENSION = '.py'


class ProjectValidator(object):

    @staticmethod
    def modules_checkup(root):
        print "modules_checkup START"

        if not os.path.exists(root):
            raise Exception('modules_checkup: {} project folder was not found'.format(root))

        start_module, start_class = None, None
        module_names = os.listdir(root)
        for module_name in module_names:
            file_name = os.path.join(root, module_name)
            if file_name.endswith('.pyc'):
                continue
            if os.path.isdir(file_name):
                continue
            if file_name.endswith('.xlsx'):
                continue
            # if not file_name.endswith(PYTHON_EXTENSION):
            #     raise Exception('modules_checkup:  Only python files (.py, .pyc) allowed, {} was found'.format(file_name))

            if file_name.find('__init__') == -1:
                name = os.path.basename(file_name)[:-len(PYTHON_EXTENSION)]
                module = imp.load_source(name, file_name)
                class_name = ProjectValidator.source_check(module)
                if class_name is not None:
                    if start_module is not None or start_class is not None :
                        raise Exception('modules_checkup: more than one starting file found {}'.format(file_name))
                    start_module = module
                    start_class = class_name
                    print 'Starting class found: {} {}'.format(start_module, start_class)

        if start_module is None or start_class is None:
            raise Exception("modules_checkup: start point wasn't found")

        print "modules_checkup FINISH"


    @staticmethod
    def source_check(module):
        source_lines = inspect.getsourcelines(module)[0]
        valid_inheritance = valid_load_specific_data = valid_run_project_calculation = False
        class_name = None
        valid_file = False

        for i, line in enumerate(source_lines):
            if line.find('(BaseCalculationsScript') > 0:
                valid_inheritance = True
                class_name = line[len('class') + 1:line.find('(BaseCalculationsScript')]
            # if line.find('def load_specific_data') > 0:
            #     valid_load_specific_data = True
            if line.find('def run_project_calculations') > 0:
                valid_run_project_calculation = True

            if 'global ' in line:
                print 'source_check: global variable found in {}, line={}, i={}'.format(module, line, i)
                break

            valid_file = valid_inheritance and valid_load_specific_data and valid_run_project_calculation
            if valid_file:
                break

        if not valid_file:
            print 'file {} not start module, inheritance={}, load_specific_data={}, run_project_calculations={}'.format(
                module, valid_inheritance, valid_load_specific_data, valid_run_project_calculation)
        else:
            print 'file {} is valid start module'. format(module)

        return class_name
