__author__ = 'yoava'

import ast


class UsageAnalyzer:

    def __init__(self, verbose):
        self.file_path = '/home/yoava/dev/kpi_factory/Projects/PERFETTICN/Utils/KPIToolBox.py'
        self.verbose = verbose
        self.file_dict = self.insert_file_to_dict()
        self.used_imports = self.find_kpi_utils_v2_usage()

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

    def find_kpi_utils_v2_usage(self):
        """
        this method finds the imports that are being used from kpi_utils_v2
        :return: list off imports
        """
        kpi_utils_imports = []
        for x in self.file_dict.values():
            if x.__contains__('KPIUtils_v2') and not x.__contains__('#'):
                imp = x.split('import')
                kpi_utils_imports.append(str(imp[1]).strip(' ').strip("\n"))
        return kpi_utils_imports

    def find_names_in_init(self):
        """
        this method finds the names of the imports as they are given in the 'init' method
        :return: list of import names
        """
        actual_import_names = []
        with open(self.file_path) as f:
            tree = ast.parse(f.read())
            for exp in tree.body:
                # check if we are inside a class now
                if isinstance(exp, ast.ClassDef):
                    for class_exp in exp.body:
                        # check if we are inside a class method
                        if isinstance(class_exp, ast.FunctionDef):
                            if class_exp.name == '__init__':
                                first_line = class_exp.lineno + 1
                                last_line = first_line + len(class_exp.body)
                                # iterate init method
                                for i in range(first_line, last_line + 1):
                                    for imp in self.used_imports:
                                        if str(self.file_dict[i]).__contains__(str(imp)):
                                            def_str = self.file_dict[i].split('=')[0].strip(' ')
                                            actual_import_names.append(def_str)
                                break
        f.close()
        return actual_import_names

    def find_usage_inside_functions(self, func, imports):
        usage_counter = 0
        line_number = func.lineno
        final_line = func.body[len(func.body)-1].lineno
        for line in range(line_number, final_line):
            for name in imports:
                if str(self.file_dict[line]).__contains__(name) and func.name != '__init__':
                    if self.verbose:
                        print "import {0} appears in line {1} in function {2}".format(name, line + 1, func.name)
                    usage_counter += 1
        return usage_counter

    def iterate_file(self, imports):
        num_of_functions = 0
        usage_number = 0
        with open(self.file_path) as f:
            tree = ast.parse(f.read())

            for exp in tree.body:
                if isinstance(exp, ast.FunctionDef):
                    print exp
                    num_of_functions += 1
                    usage_number += self.find_usage_inside_functions(exp, imports)
                if isinstance(exp, ast.ClassDef):
                    for class_exp in exp.body:
                        if isinstance(class_exp, ast.FunctionDef):
                            usage_number += self.find_usage_inside_functions(class_exp, imports)
                    num_of_functions += sum(isinstance(class_exp, ast.FunctionDef) for class_exp in exp.body)

        f.close()
        return usage_number


if __name__ == '__main__':
    analyzer = UsageAnalyzer(verbose=False)
    names = analyzer.find_names_in_init()
    print names
    usages = analyzer.iterate_file(names)
    print "in file {0} there are {1} usages of kpi_utils_lvl2".format(analyzer.file_path , usages)

