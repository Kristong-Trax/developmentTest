# -*- coding: utf-8 -*-

from Trax.Utils.Logging.Logger import Log
from KPIUtils.Utils.Validators.Template.TemplateValidator import TemplateValidator
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


def templateValidator():
    # pay attention if there any number column in template like product en please put string in the last column
    project_name = 'ccbottlersus'
    helper_path = '~/dev/trax_ace_factory/Projects/CCBOTTLERSUS/REDSCORE/Data/helper.xlsx'
    file_path = '~/dev/trax_ace_factory/Projects/CCBOTTLERSUS/REDSCORE/Data/MANUAL RED SURVEY_COKE_UNITED_RS_KPI_Template_v3.0.xlsx'
    validate_file_path = '/home/Israel/Desktop/validate_ccbottlersus.xlsx'
    validation_object = TemplateValidator(project_name, helper_path, file_path, validate_file_path)
    validation_object.run()

# if __name__ == '__main__':
#     LoggerInitializer.init('start template validate')
#     templateValidator()
