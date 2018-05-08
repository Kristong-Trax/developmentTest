from Projects.CCZA.Utils.Const import Const


class Converters(object):

    @staticmethod
    def convert_type(type_name):
        """
        :param type_name: a string from the template.
        :return: the string in the DB.
        """
        if type_name in Const.templateNames_realFieldNames:
            return Const.templateNames_realFieldNames[type_name]
        return type_name
