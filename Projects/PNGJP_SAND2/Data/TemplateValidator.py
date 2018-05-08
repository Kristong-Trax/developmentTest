# -*- coding: utf-8 -*-
import os
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.Utils.Validators.Template.TemplateValidator import TemplateValidator

def templateValidator():
    # pay attention if there any number column in template like product en please put string in the last column
    project_name = 'pngjp_sand2'
    helper_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '', 'helper.xlsx')
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '', 'Template.xlsx')
    validate_file_path = '/home/Israel/Desktop/validate_pngjp.xlsx'
    validation_object = TemplateValidator(project_name, helper_path, file_path, validate_file_path)
    validation_object.run()

# if __name__ == '__main__':
#     LoggerInitializer.init('start template validate')
#     templateValidator()
