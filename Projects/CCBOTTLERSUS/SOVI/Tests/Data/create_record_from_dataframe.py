import json
import os
import pandas as pd


class Consts:

    WORDS_TO_REPLASE = {'NaN': 'None',
                        'null': 'None',
                        'false': 'False',
                        'true': 'True'}


class CreateDictFromDf:
    def __init__(self, destination_file_name, destination_file_path, df, dictionary_name, replace_word_list=None):
        self.des_file_name = destination_file_name
        self.des_file_path = destination_file_path
        self.df = df
        self.replace_word_list = replace_word_list
        self.dict_str_to_save = ''
        self.dictionary_name = dictionary_name

    def main_calculation(self):
        self.fix_df_dates()
        self.convert_dict_to_str()
        self.replace_words()
        self.save_dict_in_file()

    def fix_df_dates(self):
        df_columns_list = self.df.columns.to_list()
        df_columns_list_with_date = []
        if type(df_columns_list[0]) == int:
            return
        for i in df_columns_list:
            if ('date' in i) or ('time' in i):
                df_columns_list_with_date.append(i)
        # convert date object to string
        for col in df_columns_list_with_date:
            self.df[col] = self.df[col].astype('str')

    def convert_df_to_dict(self):
        return self.df.to_dict()

    def convert_dict_to_str(self):
        str_dict = self.convert_df_to_dict()
        str_dict = self.dictionary_name + ' = ' + json.dumps(str_dict, indent=2) + '\n\n'
        self.dict_str_to_save = str_dict

    def replace_words(self):
        self.replace_function(Consts.WORDS_TO_REPLASE)
        if self.replace_word_list:
            self.replace_function(self.replace_word_list)

    def replace_function(self, replace_dict_):
        replace_dict = replace_dict_
        words_to_replace = replace_dict.keys()
        for w in words_to_replace:
            self.dict_str_to_save = self.dict_str_to_save.replace(w, replace_dict[w])

    def save_dict_in_file(self):
        the_path = os.path.join(self.des_file_path, self.des_file_name)
        file_to_use = open(the_path, 'a')
        file_to_use.write(self.dict_str_to_save)
        file_to_use.close()

# if __name__ == "__main__":
#     file_to_read = 'session_info'
#     df = pd.read_csv(file_to_read + '.csv')
#     a = CreateDictFromDf('records.py', os.path.dirname(os.path.abspath(__file__)), df, file_to_read)
#     a.main_calculation()