import subprocess
import os
import pandas as pd
from StringIO import StringIO
import argparse
from Trax.Utils.Conf.Configuration import Config


COLUMN_PROJECT_NAME = 'Project_Name'
COLUMN_PYTHON_FILE = 'Python Class Path'
COLUMN_REFERENCED_FOLDER = 'Referred_Folder'
COLUMN_REFERRED_FILE = 'Imports Path'


class ValidateRelations(object):
    def __init__(self):
        self.parsed_arguments = ValidateRelations.parse_arguments()
        self.projects_path = self.parsed_arguments.projects_path
        self.sfood_home = os.path.join(os.environ['HOME'], 'miniconda/envs/garage/bin/sfood')
        self.projects_list = None
        self.output_path = None
        self.error_file = []
        if getattr(self.parsed_arguments, 'projects'):
            self.projects_list = map(lambda x: x.upper(), self.parsed_arguments.projects.split(','))
        if getattr(self.parsed_arguments, 'output_path'):
            self.output_path = self.parsed_arguments.output_path

    @staticmethod
    def parse_arguments():
        """
            In this method we have all the dedicated arguments this class receives
        :return: parse args object
        """
        parser = argparse.ArgumentParser(description='Loads the Assortment Template file '
                                                     'into the respective DB Schema')
        parser.add_argument('--projects_path', '-pp', type=str, required=True, help='The project folder path')
        parser.add_argument('--projects', '-p', type=str, required=False, help='The projects name')
        parser.add_argument('--output_path', '-op', type=str, required=False, help='The file output path')
        return parser.parse_args()

    @staticmethod
    def validate_relations(project, relation_file):
        test = StringIO(relation_file)
        df = pd.read_csv(test, names=[COLUMN_PROJECT_NAME,
                                      COLUMN_PYTHON_FILE,
                                      COLUMN_REFERENCED_FOLDER,
                                      COLUMN_REFERRED_FILE],
                         )
        df.reset_index(drop=True)
        cleaned_df = ValidateRelations.cleanDataFrame(project, df)
        return cleaned_df

    @staticmethod
    def cleanDataFrame(project, df):
        df = df[df[COLUMN_REFERENCED_FOLDER] != 'None']  # Remove Empty entries
        df = df[df[COLUMN_REFERENCED_FOLDER].str.contains('theGarage') == False]  # Remove Garage Ref
        df = df[df.Referred_Folder.str.contains('miniconda') == False]  # Remove miniconta/python lib
        df = df[df[COLUMN_REFERRED_FILE].str.contains('KPIUtils') == False]  # Remove KPIUtils
        df = df[df[COLUMN_REFERRED_FILE].str.contains(project + "/") == False]  # Remove same path
        return df

    def log_errors(self, project, lines):

        if lines and len(lines) > 0:
            self.error_file.append("Importing Error have been found for :***{project}***\n".format(project=project))
            self.error_file += lines

    def excecute(self):

        try:
            if not self.projects_list:
                projects = os.listdir(self.projects_path)
            else:
                projects = self.projects_list

            maindf = pd.DataFrame(columns=[COLUMN_PROJECT_NAME,
                                           COLUMN_PYTHON_FILE,
                                           COLUMN_REFERENCED_FOLDER,
                                           COLUMN_REFERRED_FILE])
            projects.sort()
            for project in projects:
                project_x = os.path.join(self.projects_path, project.upper())
                if os.path.isdir(project_x):
                    if not project in['CCKH']:
                        print "***Validating Imports for: {}*****".format(project_x)
                        p = subprocess.Popen([self.sfood_home, project_x], stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
                        print "1"
                        #self.log_errors(project, p.stderr.readlines())
                        result = p.communicate()[0]
                        print "2"
                        res_df = ValidateRelations.validate_relations(project,
                                                                      result.replace("(", "").
                                                                      replace(")", "").
                                                                      replace("'", "").
                                                                      replace(" ", ""))
                        print "3"
                        maindf = maindf.append(res_df, ignore_index=True)

            export_df = maindf.drop([COLUMN_PROJECT_NAME, COLUMN_REFERENCED_FOLDER], axis=1)
            if len(maindf) > 0 or len(self.error_file) > 0:
                if self.output_path:
                    with open(os.path.join(self.output_path, 'bad_relations.csv'), "wb") as output_file:
                        if len(maindf) > 0:
                            print "ERROR: The bellow 'Python Classes' are trying to import classes from other Projects.\n" \
                                  "Please correct this as imports are only allowed to be inside the same Project and/or to the KPIUtils folders.\n"
                            export_df.to_csv(path_or_buf=output_file,
                                             encoding='utf-8',
                                             index=False)
                        if self.error_file > 0:
                            output_file.writelines("".join(self.error_file))
                    print "Import validation out file:{}".format(os.path.join(self.output_path, 'bad_relations.csv'))
                if len(maindf) > 0:
                    pd.set_option('display.width', 1024)
                    pd.set_option('max_colwidth', 800)
                    print export_df.to_string(index=False)
                    print "\n"
                if self.error_file > 0:
                    print "".join(self.error_file)
                exit(1)
            else:
                print "Import validations have been passed successfully"
        except Exception as e:
            print "Import validation execution have thrown an exception"
            print e
            exit(1)
        finally:
            print "*****************************Finished Import Validation****************************"


if __name__ == '__main__':
    Config.init()
    print "*****************************Running Import Validation******************************"
    import_validation = ValidateRelations()
    import_validation.excecute()
