import os

import findspark
import pyspark
import pandas as pd
import matplotlib.pyplot as plt
import shutil
import webbrowser

from pyspark.sql import SparkSession ,functions as F
from Trax.Utils.Conf.Configuration import Config
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


ROOT_RESULT_FOLDER = "results"
HISTOGRAM_FOLDER = ROOT_RESULT_FOLDER + "/histogram"
RAW_DATA = ROOT_RESULT_FOLDER + "/raw"
SUMMERY_FILE = os.path.join(ROOT_RESULT_FOLDER, "test_results.html")


class qa:
    def __init__(self, project, batch_size=300000, start_date=None , end_date=None ,config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config'):

        if os.path.exists("results"):
            shutil.rmtree(ROOT_RESULT_FOLDER)

        os.mkdir(ROOT_RESULT_FOLDER)
        os.mkdir(HISTOGRAM_FOLDER)
        os.mkdir(RAW_DATA)

        # if os.path.isfile(SUMMERY_FILE):
        #     os.remove(SUMMERY_FILE)

        findspark.init('/home/ilanp/miniconda/envs/garage/lib/python2.7/site-packages/pyspark')
        findspark.add_jars('/usr/share/java/mysql-connector-java.jar')
        self.spark = SparkSession.builder.appName("qa_tool").config("spark.driver.memory","4g")\
                                                            .config("spark.executor.memory", "4g")\
                                                            .config("spark.driver.cores", "4")\
                                                            .config("spark.driver.maxResultSize", "4g")\
                                                            .config("spark.ssl.enabled","True") \
                                                            .config("spark.ssl.protocol", "TLSv1.1").getOrCreate()

        self._project = project
        self._config_file = config_file
        self._dbUser = DbUsers.CalculationEng
        self._env = Config.PROD
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size
        self.connector = PSProjectConnector(self._project,self._dbUser)
        self.project_url = 'jdbc:mysql://{}/report'.format(self.connector.project_params['rds_name'])

        #const
        self.results_query = '''    
                            (SELECT 
                                report.kpi_level_2_results.*,
                                probedata.session.pk as sessions_pk
                            FROM
                                report.kpi_level_2_results,
                                probedata.session
                            WHERE
                                probedata.session.pk = report.kpi_level_2_results.session_fk
                                    AND probedata.session.visit_date BETWEEN '{}' AND '{}')tmp_kpi_level_2_results '''.format(
            start_date, end_date)

        self.scene_results_query = '''    
                            (SELECT 
                                report.scene_kpi_results.*
                            FROM
                                report.scene_kpi_results,
                                probedata.session
                            WHERE
                                probedata.session.pk = report.scene_kpi_results.session_fk
                                    AND probedata.session.visit_date BETWEEN '{}' AND '{}')tmp_scene_kpi_results '''.format(
            start_date, end_date)

        self.static_query = '''(SELECT * FROM  static.kpi_level_2 where static.kpi_level_2.kpi_calculation_stage_fk = 3) static_kpi '''
        self.categories_query = '''(select * from static_new.category) categories '''

        # fetch db data
        self.static_kpi = self._get_static_kpi()
        self.kpi_results = self._get_kpi_results()
        # self.kpi_scene_results = self._get_kpi_scene_results()
        self.categories_df = self._get_categories()
        # self.merged_kpi_results = []
        self.expected = pd.read_csv('expected.csv')


    def _get_merged_session_kpi_result(self):
        pass

    def _get_kpi_results_meta_data(self):
        try:
            meta_results = '''    
                        SELECT 
                            count(*) count, 
                            MIN(probedata.session.pk) min,
                            MAX(probedata.session.pk) max
                        FROM
                            report.kpi_level_2_results,
                            probedata.session
                        WHERE
                            probedata.session.pk = report.kpi_level_2_results.session_fk
                                AND probedata.session.visit_date BETWEEN '{}' AND '{}';'''.format(self.start_date, self.end_date)

            return pd.read_sql_query(meta_results, self.connector.db)
        except Exception as e:
            print e.message

    def _get_kpi_scene_results_meta_data(self):
        try:
            meta_results = '''    
                        SELECT 
                            count(*) count, 
                            MIN(probedata.session.pk) min,
                            MAX(probedata.session.pk) max
                        FROM
                            report.scene_kpi_results,
                            probedata.scene
                        WHERE
                            probedata.scene.pk = report.scene_kpi_results.scene_fk
                                AND probedata.session.visit_date BETWEEN '{}' AND '{}';'''.format(self.start_date, self.end_date)

            return pd.read_sql_query(meta_results, self.connector.db)
        except Exception as e:
            print e.message

    def _get_kpi_results(self):

        kpi_results_meta_data = self._get_kpi_results_meta_data()
        lower_bound = kpi_results_meta_data.loc[0]['min']
        upper_bound = kpi_results_meta_data.loc[0]['max']
        count_of_row = kpi_results_meta_data.loc[0]['count']
        if (count_of_row / self.batch_size) > 1:
            number_of_partition = (count_of_row / self.batch_size)
        else:
            number_of_partition = 1
        print "count of rows : " + str(count_of_row)
        print " number of partition:" + str(number_of_partition)

        if count_of_row > 0:
            kpi_results = self.spark.read.jdbc(url=self.project_url,
                                               table=self.results_query,
                                               properties={"user": self.connector.dbuser.username,
                                                           "password": self.connector.dbuser.cred,
                                                           "partitionColumn": "sessions_pk",
                                                           "lowerBound": "{}".format(lower_bound),
                                                           "upperBound": "{}".format(upper_bound),
                                                           "numPartitions": "{}".format(number_of_partition),
                                                           "driver": 'com.mysql.jdbc.Driver'}) \
                                            .persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

            kpi_results.count()
            return kpi_results

        else:
            print("no kpi session results")
            return None

    def _get_kpi_scene_results(self):

        kpi_results_meta_data = self._get_kpi_scene_results_meta_data()
        lower_bound = kpi_results_meta_data.loc[0]['min']
        upper_bound = kpi_results_meta_data.loc[0]['max']
        count_of_row = kpi_results_meta_data.loc[0]['count']
        if (count_of_row / self.batch_size) > 1:
            number_of_partition = (count_of_row / self.batch_size)
        else:
            number_of_partition = 1
        print "count of rows : " + str(count_of_row)
        print " number of partition:" + str(number_of_partition)

        if count_of_row > 0:
            kpi_results = self.spark.read.jdbc(url=self.project_url,
                                               table=self.results_query,
                                               properties={"user": self.connector.dbuser.username,
                                                           "password": self.connector.dbuser.cred,
                                                           "partitionColumn": "tmp_scene_kpi_results.scene_fk",
                                                           "lowerBound": "{}".format(lower_bound),
                                                           "upperBound": "{}".format(upper_bound),
                                                           "numPartitions": "{}".format(number_of_partition),
                                                           "driver": 'com.mysql.jdbc.Driver'}).persist(
                storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

            kpi_results.count()
            return kpi_results
        else:
            print("no scene results")
            return None

    def _get_static_kpi(self):
        static_kpi = self.spark.read.jdbc(url=self.project_url,
                                          table=self.static_query,
                                          properties={"user": self.connector.dbuser.username,
                                                      "password":  self.connector.dbuser.cred,
                                                      "driver": 'com.mysql.jdbc.Driver'})\
                                    .persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

        static_kpi.count()
        return static_kpi

    def _get_categories(self):
        categories = self.spark.read.jdbc(url=self.project_url,
                                          table=self.categories_query,
                                          properties={"user": self.connector.dbuser.username,
                                                      "password": self.connector.dbuser.cred,
                                                      "driver": 'com.mysql.jdbc.Driver'})\
                                    .persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

        categories.count()
        return categories

    def test_uncalculated_session_kpi(self):
        """get list of kpi names that doesnt have any result """

        filtered = self.merged_kpi_results.filter('result is null')
        df = filtered.groupBy("client_name", "result").count()\
                                                      .withColumnRenamed('count', 'result_count')\
                                                      .withColumnRenamed('client_name', 'name')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session count"))
        test_results = df.join(df2, df2.client_name == df.name)

        print '## uncalculated kpi list ##'
        test_results.show(1000, False)
        test_results_pandas = test_results.select("client_name","session count").toPandas()
        test_results_pandas.to_csv(RAW_DATA + "/uncalculated_kpi_list.csv" , encoding="utf8")
        return test_results_pandas.to_html(classes=["table","table-striped","table-hover"])

    def test_invalid_percent_results(self):
        """kpi result should be percent between 0-1 in"""
        total_sessions = self.merged_kpi_results.select("session_fk").distinct().count()
        filtered = self.merged_kpi_results.filter('is_percent = 1 and (result < 0 or result > 1)')
        df = filtered.groupBy("client_name").count() \
                                            .withColumnRenamed('count', 'result_count') \
                                            .withColumnRenamed('client_name', 'name')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session_count"))
        test_results = df.join(df2, df2.client_name == df.name)

        # write  detailed result
        test_results.write.csv(RAW_DATA + "/test_invalid_percent_results", header=True)
        test_results_pandas = test_results.select('client_name', \
                                                  'result_count', \
                                                  "session_count", \
                                                  ((F.col('session_count') / total_sessions) * 100).alias("session_count%"))\
                                                   .toPandas()
        test_results_pandas.to_csv(RAW_DATA + "/invalid_percent_results_list.csv" , encoding="utf8")
        return test_results_pandas.to_html(classes=["table","table-striped","table-hover"])

    def test_result_is_zero(self):
        """ kpi with results is 0 """

        total_sessions = self.merged_kpi_results.select("session_fk").distinct().count()
        filtered = self.merged_kpi_results.filter('result == 0 ')
        df = filtered.groupBy('client_name').count().withColumnRenamed('count', 'results_zero_count').withColumnRenamed(
            'client_name', 'name')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session_count"))
        df3 = self.merged_kpi_results.groupBy('client_name').count().withColumnRenamed('count', 'total_count') \
                                                                    .withColumnRenamed('client_name', 'name2')
        test_results = df.join(df2, df2.client_name == df.name).join(df3, df3.name2 == df.name)

        # write  detailed result
        test_results.write.csv( RAW_DATA + "/test_result_is_zero", header=True)

        test_results_pandas = test_results.select('client_name', \
                                                  'results_zero_count', \
                                                  ((F.col('results_zero_count') / F.col('total_count')) * 100).alias(
                                                  "results%(out of all results)"), \
                                                  ((F.col('session_count') / total_sessions) * 100).alias(
                                                  "session_count%(out of all sessions)") \
                                                  ).toPandas()
        test_results_pandas.to_csv(RAW_DATA + "/test_result_is_zero.csv" , encoding="utf8")
        return test_results_pandas.to_html(classes=["table","table-striped","table-hover"])

    def test_results_stdev(self):
        test_results_pandas = self.merged_kpi_results.groupBy('client_name').agg(F.stddev('result'), \
                                                                         F.mean('result'), \
                                                                         F.min('result'), \
                                                                         F.max('result')).orderBy('client_name').toPandas()
        test_results_pandas.to_csv(RAW_DATA + "/test_results_stdev.csv" , encoding="utf8")
        return test_results_pandas.to_html(classes=["table","table-striped","table-hover"])

    def test_results_in_expected_range(self):
        """ show list or results that are not in the expected range"""

        for i, row in self.expected.iterrows():
            x = self.merged_kpi_results.loc[(self.merged_kpi_results['client_name'] == row['kpi_name']) & (
                        (self.merged_kpi_results['result'] < row['min']) | (self.merged_kpi_results['result'] > row['max']))]
            print "##### results not in expected range: (" + str(row['min']) + '-' + str(row['max']) + ' )' + row['kpi_name']
            print x[['session_fk', 'client_name', 'result']]

    def test_results_by_category_stddev(self):
        merged_kpi_results_tmp = self.merged_kpi_results.filter('denominator_type_fk = 4')
        categories_results = merged_kpi_results_tmp.join(self.categories_df,
                                                         self.categories_df.pk == merged_kpi_results_tmp.denominator_id)
        test_results_pandas = categories_results.select(categories_results.client_name , \
                                                        categories_results.name.alias("category_name") , \
                                                        categories_results.result).groupBy('client_name',"category_name")\
                                                                    .agg(F.stddev('result'), \
                                                                         F.mean('result'), \
                                                                         F.min('result'), \
                                                                         F.max('result')).orderBy('client_name').toPandas()

        test_results_pandas.to_csv(RAW_DATA + "/test_results_by_category_stddev.csv" , encoding="utf8")
        return test_results_pandas.to_html(classes=["table","table-striped","table-hover"])

    def gen_kpi_histogram(self):

        static = self.static_kpi.toPandas()

        for i, row in static.iterrows():
            print "*** {} ***".format(row['client_name'].encode('utf-8').strip())
            if row['client_name'].find('"') > 0 :
                print "invalid char cant create histogram  *** {} ***".format(row['client_name'])
                continue
            filter = 'result is not null  and client_name = "{}"'.format(row['client_name'].encode('utf-8'))
            res = self.merged_kpi_results.filter(filter).select('client_name','result').toPandas()
            # res = kpi.loc[(kpi['client_name'] == row['client_name'])]
            if not res.empty:
                res[['client_name', 'result']].hist()
                plt.title(row['client_name'])
                file_name = HISTOGRAM_FOLDER + "/" + row['client_name'].replace("/","_") + ".png"
                plt.savefig(file_name)
                plt.close()

                with open(SUMMERY_FILE, 'a') as file:
                    url = "histogram" + "/" + row['client_name'].replace("/","_") + ".png"
                    file.write("<img src='{}' >".format(url.encode('utf-8')))

    @staticmethod
    def start_html_report():
        with open(SUMMERY_FILE, 'a') as report:
            head = '''
                   <!doctype html>
                    <html lang="en">
                      <head>
                        <!-- Required meta tags -->
                        <meta charset="utf-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
                    
                        <!-- Bootstrap CSS -->
                        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css" integrity="sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO" crossorigin="anonymous">
                       
                        <!-- --> 
                        <style>
                        body {font-family: "Roboto", "Lato", "Helvetica Neue", "Helvetica", "Arial" } ;
                        
                        </style>
                        
                        
                        <title>QA Tool Results</title>
                        
                        
                        
                      </head>
                      <body>            
                    '''
            report.write(head)

    @staticmethod
    def end_html_report():
        with open(SUMMERY_FILE, 'a') as report:
            end = '''
                      <!-- Optional JavaScript -->
                        <!-- jQuery first, then Popper.js, then Bootstrap JS -->
                        <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>
                        <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.3/umd/popper.min.js" integrity="sha384-ZMP7rVo3mIykV+2+9J3UJ46jBk0WLaUAdn689aCwoqbBJiSnjAK/l8WvCWPIPm49" crossorigin="anonymous"></script>
                        <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/js/bootstrap.min.js" integrity="sha384-ChfqqxuZUCnJSK3+MXmPNIyE6ZbWh2IMqE241rYiqJxyMiZ6OW/JmZQ5stwEULTy" crossorigin="anonymous"></script>
                      </body>
                    </html>            
                                      
                  '''
            report.write(end)

    def get_statistics(self):

        stats = """ 
                <p>
                <br>
                   <br>
                    Project Name:  {project}  <br>
                    Dates:  {start_date} - {end_date}  <br>    
                    Total Results: {total_results}   <br>    
                    Total Sessions: {total_sessions}   <br>    
                    Total Kpi: {total_kpi}   <br>       
                    <br>
                </p>

              """.format(project=self._project,
                         start_date=self.start_date,
                         end_date=self.end_date,
                         total_results=self.merged_kpi_results.select("result").count(),
                         total_sessions=self.merged_kpi_results.select("session_fk").distinct().count(),
                         total_kpi=self.static_kpi.select("client_name").distinct().count())

        print stats
        with open(SUMMERY_FILE, 'a') as file:
            file.write(stats)

    def run_all_tests(self):

        '''
        run full test report
        :return:
        '''

        self.start_html_report()
        # session kpi tests
        self.static_kpi = self.static_kpi.filter('session_relevance == 1')
        self.merged_kpi_results = self.static_kpi.join(self.kpi_results,
                                                       self.static_kpi.pk == self.kpi_results.kpi_level_2_fk,
                                                       how='left')

        with open(SUMMERY_FILE, 'a') as file:
            file.write("<div class='container' ")
            file.write("<br> <h1 class='text-center'> Session KPI</h1>")
            self.get_statistics()
            file.write("<br> <h2 class='text-center'> test invalid percent results</h2>")
            file.write(self.test_invalid_percent_results())

            file.write("<br> <h2 class='text-center'> test uncalculated kpi</h2>")
            file.write(self.test_uncalculated_session_kpi())

            file.write("<br> <h2 class='text-center'> test result is zero</h2>")
            file.write(self.test_result_is_zero())

            file.write("<br> <h2 class='text-center'> test results stddev</h2>")
            file.write(self.test_results_stdev())

            file.write("<br> <h2 class='text-center'> test results by category stddev</h2>")
            file.write(self.test_results_by_category_stddev())
            file.write("</div>")

            # file.write(self.test_results_in_expected_range())
            # file.write(self.test_one_result_in_all_sessions())

            file.write("<h2 class='text-center'>Results Histogram</h2>")

        self.gen_kpi_histogram()

        # scene kpi tests

        self.end_html_report()

    #TODO
    # scene kpi results
    # input of session list



if __name__ == "__main__":
    Config.init(app_name='ttt', default_env_and_cloud = ('prod','AWS'),
                config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config')
    qa_tool = qa('jnjit', start_date='2018-12-01', end_date='2018-12-15')
    qa_tool.run_all_tests()
    webbrowser.open(os.path.join(os.getcwd(),SUMMERY_FILE))
