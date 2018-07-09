__author__ = 'yoava'

import ast
from KPIUtils_v2 import Calculations as calcs

class UsageAnalyzer:

    def __init__(self):
        self.file_path = '/home/yoava/dev/kpi_factory/Projects/INBEVTRADMX/Utils/KPIToolBox.py'
        self.file_dict = self.insert_file_to_dict()

    def insert_file_to_dict(self):
        file_dict = {}
        with open(self.file_path) as fp:
            for i, line in enumerate(fp):
                file_dict[i] = str(line)
        fp.close()
        return file_dict

    def find_kpi_utils_v2_usage(self):
        for x in self.file_dict.values():
            if x.__contains__('KPIUtils_v2') and not x.__contains__('#'):
                print x

    def find_usage_inside_functions(self, func, num_of_functions):
        line_number = func.lineno
        actual_line_number = 0
        func_len = len(func.body)
        print func.name, line_number
        for b in func.body:
            if isinstance(b, ast.Expr):
                continue
            else:
                actual_line_number = b.lineno
                print actual_line_number
                break
        print 'function {0} starts at line {1} and ends in line {2}'.format(func.name, line_number,
                                                                            actual_line_number + func_len - 1)
        if func.name != '__init__':
            num_of_functions += 1

    def iterate_file(self):
        num_of_functions = 0
        with open(self.file_path) as f:
            tree = ast.parse(f.read())

            for exp in tree.body:
                if isinstance(exp, ast.FunctionDef):
                    print exp
                    num_of_functions += 1
                if isinstance(exp, ast.ClassDef):
                    for class_exp in exp.body:
                        if isinstance(class_exp, ast.FunctionDef):
                            self.find_usage_inside_functions(class_exp, num_of_functions)

                    num_of_functions += sum(isinstance(class_exp, ast.FunctionDef) for class_exp in exp.body)

        print num_of_functions


if __name__ == '__main__':
    analyzer = UsageAnalyzer()
    analyzer.iterate_file()
