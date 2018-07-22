__author__ = 'yoava'

import ast
import os
import pandas as pd
# from KPIUtils_v2.Utils.Decorators.Decorators import log_task

"""
this script is used for finding number of usages of functions from kpi_utils_v2 in kpi_factory projects, 
and what has been used
"""


class UsageAnalyzer:

    def __init__(self, file_path, verbose=False):
        self.file_path = file_path
        self.verbose = verbose
        self.file_dict = self.insert_file_to_dict()
        self.kpi_utils_used_imports = self.find_kpi_utils_v2_usage('KPIUtils')
        self.kpi_utils_v2_used_imports = self.find_kpi_utils_v2_usage('KPIUtils_v2')

    def insert_file_to_dict(self):
        """
        this method iterates the file and saves it as a dict where key is line number and value is the line itself
        :return: dictionary
        """
        file_dict = {}
        with open(self.file_path) as fp:
            for i, line in enumerate(fp):
                file_dict[i] = str(line)
        fp.close()
        return file_dict

    def find_kpi_utils_v2_usage(self, version):
        """
        this method finds the imports that are being used from kpi_utils_v2
        :return: list off imports
        """
        kpi_utils_imports = []
        for x in self.file_dict.values():
            if version == 'KPIUtils':
                if x.__contains__(version) and not x.__contains__('#') and not x.__contains__('KPIUtils_v2'):
                    imp = x.split('import')
                    kpi_utils_imports.append(str(imp[1]).strip(' ').strip("\n"))
            else:
                if x.__contains__(version) and not x.__contains__('#'):
                    imp = x.split('import')
                    kpi_utils_imports.append(str(imp[1]).strip(' ').strip("\n"))
        return kpi_utils_imports

    def find_names_in_init(self):
        """
        this method finds the names of the imports as they are given in the 'init' method
        :return: list of import names
        """
        kpi_utils_actual_import_names = []
        kpi_utils_v2_actual_import_names = []
        with open(self.file_path) as f:
            tree = ast.parse(f.read())
            for exp in tree.body:
                # check if we are inside a class now

                if isinstance(exp, ast.ClassDef):
                    for class_exp in exp.body:
                        # check if we are inside a class method
                        if isinstance(class_exp, ast.FunctionDef):
                            if class_exp.name == '__init__':
                                self.get_actual_import_names(kpi_utils_actual_import_names, class_exp,
                                                             self.kpi_utils_used_imports)
                                self.get_actual_import_names(kpi_utils_v2_actual_import_names, class_exp,
                                                             self.kpi_utils_v2_used_imports)
                                break
        f.close()
        return kpi_utils_actual_import_names, kpi_utils_v2_actual_import_names

    def get_actual_import_names(self, actual_import_names, class_exp, import_list):
        """
        this method finds inside the actual name of the imports as they were given inside 'init' function
        :param import_list:
        :param actual_import_names:
        :param class_exp:
        :return:
        """
        first_line = class_exp.lineno + 1
        last_line = first_line + len(class_exp.body)
        # iterate init method
        for i in range(first_line, last_line + 1):
            for imp in import_list:
                if str(self.file_dict[i]).__contains__(str(imp)):
                    def_str = self.file_dict[i].split('=')[0].strip(' ')
                    actual_import_names.append(def_str)

    def find_usage_inside_function(self, func, imports):
        """
        this method find the number of usages from kpi_utils_v2 imports inside function
        :param func: function / class method to check
        :param imports: imports from kpi_utils
        :return: number of usages
        """
        used_imports = set()
        usage_counter = 0
        line_number = func.lineno
        final_line = func.body[len(func.body)-1].lineno
        for line in range(line_number, final_line):
            for name in imports:
                if str(self.file_dict[line]).__contains__(name) and func.name != '__init__':
                    if self.verbose:
                        print "import {0} appears in line {1} in function {2}".format(name, line + 1, func.name)
                    usage_counter += 1
                    used_imports.add(name)
        return usage_counter, used_imports

    # def find_function_from_import(self, import_line, name):
    #     line = import_line.strip(" ")
    #     relevant_part = line[line.find(name): len(line)]
    #     a = relevant_part.split('(')[0]
    #     if len(a.split('.')) == 3:
    #         print a.split('.')[2]
    #     # print a, len(a.split('.'))
    #     # b = a.split['.'][2]
    #     # print b

    def iterate_file(self, import_list):
        """
        this method iterates the file and finds number of usages from kpi_utils and and used usages
        :param import_list: imports from kpi_utils
        :return:
        """
        usage_number, imports_number = 0, 0
        imports_set = set()
        with open(self.file_path) as f:
            tree = ast.parse(f.read())
            for exp in tree.body:
                if isinstance(exp, ast.FunctionDef):
                    usage_number = self.get_number_of_usages_and_imports(exp, import_list, imports_set, usage_number)
                if isinstance(exp, ast.ClassDef):
                    for class_exp in exp.body:
                        if isinstance(class_exp, ast.FunctionDef):
                            usage_number = self.get_number_of_usages_and_imports(class_exp, import_list, imports_set,
                                                                                 usage_number)
        f.close()
        return usage_number, imports_set

    def get_number_of_usages_and_imports(self, class_exp, imports, imports_set, usage_number):
        """
        this method counts number of usages in function and updates usages set
        :param class_exp: function
        :param imports: all imports
        :param imports_set: used imports
        :param usage_number: number of usages in class
        :return: usage number
        """
        imports_number, used_imports = self.find_usage_inside_function(class_exp, imports)
        usage_number += imports_number
        imports_set.update(used_imports)
        return usage_number

    def run(self):
        kpi_utils_import_names, kpi_utils_v2_import_names = self.find_names_in_init()
        usages_v1, import_set_v1 = self.iterate_file(kpi_utils_import_names)
        usages_v2, import_set_v2 = self.iterate_file(kpi_utils_v2_import_names)
        print self.file_path
        print "There are {0} usages of kpi_utils in file ".format(usages_v1)
        print "There are {0} usages of kpi_utils_v2 in file".format(usages_v2)
        # if usages > 0:
        #     print "There are {0} usages of kpi_utils_v2 in file {1} ".format(usages, self.file_path)
        # else:
        #     print "No usages in file {0}".format(self.file_path)
        return project, usages_v1, import_set_v1, usages_v2, import_set_v2


if __name__ == '__main__':
    projects_dir = '/home/yoava/dev/kpi_factory/Projects'
    # all_imports = set()
    df = pd.DataFrame(columns=['Project_name', 'number_of_uses_v1', 'used_imports_v1', 'number_of_uses_v2',
                               'used_imports_v2'])
    for project in os.listdir(projects_dir):
        path = os.path.join(projects_dir, project)
        if os.path.isdir(path):
            tool_box = os.path.join(path, 'Utils', 'KPIToolBox.py')
            if os.path.exists(tool_box):
                analyzer = UsageAnalyzer(file_path=tool_box, verbose=False)
                # log_task(action='import_checker', message='import for project', environment='prod',
                #          project_name=project.lower(), user_name=os.environ.get('USER'))
                project, usages_1, import_set_1, usages_2, import_set_2 = analyzer.run()
                # all_imports.update(imports)
                json = {'Project_name': project, 'number_of_uses_v1': usages_1, 'used_imports_v1': str(import_set_1),
                        'number_of_uses_v2': usages_2, 'used_imports_v2': import_set_2}
                df = df.append(json, ignore_index=True)

    writer = pd.ExcelWriter('/home/yoava/Documents/usage.xlsx')
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    # for im in all_imports:
    #     print im
