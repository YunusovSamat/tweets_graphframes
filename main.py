from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from graphframes import GraphFrame


class TweetsGraphFrames:
    def __init__(self):
        self.g = None

    # Метод для записи в переменную, выборочные данные из файла.
    def set_graphframe(self, path):
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        # Чтение данных и разбиение на колонки.
        tweets = spark.read.csv(path, header=True, escape='\"')
        v = tweets.select('tweetid', 'userid').toDF('id', 'userid')
        e = tweets.select('in_reply_to_tweetid', 'tweetid').toDF('src', 'dst')
        e = e.filter(e.src != 'null')
        self.g = GraphFrame(v, e)

    @staticmethod
    def _check_length(length):
        if length > 0:
            return True
        else:
            return False

    @staticmethod
    def _get_chain_msg(length):
        chain_msg_list = list()
        for i in range(1, length + 1):
            chain_msg_list.append(f'(v{i})-[]->(v{i + 1})')
        chain_msg_list.append('!()-[]->(v1)')
        chain_msg_list.append(f'!(v{length + 1})-[]->()')
        return '; '.join(chain_msg_list)

    def get_found_userid(self, length):
        if self._check_length(length):
            chain_msg = self._get_chain_msg(length)
            return self.g.find(chain_msg).select('v1.userid').show()
        else:
            print('Error: length < 1')


if __name__ == '__main__':
    file_path = 'hdfs:///user/samat/ira_tweets_csv_hashed.csv'
    #file_path = 'file:///home/samat/Downloads/ira_tweets_csv_hashed.csv'
    N = 6
    tgf = TweetsGraphFrames()
    tgf.set_graphframe(file_path)
    print(tgf.get_found_userid(N))

# pyspark --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 
# spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 main.py
