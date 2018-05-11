# -*- coding: utf-8 -*-

import glob
import logging
import math
from pyspark import SparkContext
import pyspark
import os
import stat
import sys
import time


class Mode:
    local = 1
    gfs = 2


def build_users_pairs(ratings_by_movie):
    # logging.getLogger().setLevel(logging.DEBUG) logging.info('\n\nLOG TEST\n\n')
    users_ratings = ratings_by_movie[1]
    output_values = []
    for i in range(0, len(users_ratings) - 1):
        for j in range(i + 1, len(users_ratings)):
            x = users_ratings[i][0]
            y = users_ratings[j][0]
            if x <= y:
                output_values.append(((x, y), (users_ratings[i][1], users_ratings[j][1])))
            else:
                output_values.append(((y, x), (users_ratings[j][1], users_ratings[i][1])))
    return output_values


def cos_sim_and_duplication(ratings_by_users_pairs):
    x, y = ratings_by_users_pairs[0]
    numerator, norm1, norm2 = 0, 0, 0
    if len(ratings_by_users_pairs[1]) < 2:
        return iter(())
    for ratings_pair in ratings_by_users_pairs[1]:
        x_rating, y_rating = int(ratings_pair[0]), int(ratings_pair[1])
        numerator += x_rating * y_rating
        norm1 += x_rating ** 2
        norm2 += y_rating ** 2
    cos_sim = numerator / (math.sqrt(norm1) * math.sqrt(norm2)) if norm1 + norm2 > 0 else 0
    return (x, (y, cos_sim)), (y, (x, cos_sim))


# def print_fun(user_similarities):
#     x, similarities = user_similarities
#     result_file = x + '.txt'
#
#     if mode == Mode.gfs:
#         temp_folder = '/tmp/sim_output/'
#         out_file = temp_folder + result_file
#         print('\n\nGOING TO STORE FILE {} ON GCS'.format(out_file))
#         try:
#             os.makedirs(temp_folder)
#         except Exception:
#             pass
#
#         with open(out_file, 'w') as out_f:
#             for y, cos_sim in similarities:
#                 out_f.write(y + ',' + str(cos_sim) + '\n')
#     elif mode == Mode.local:
#         with open(local_output_path + result_file, 'w') as out_f:
#             for y, cos_sim in similarities:
#                 out_f.write(y + ',' + str(cos_sim) + '\n')


def prettify(user_similarities):
    x, similarities = user_similarities
    return '#' + x, '\n'.join(y + ':' + str(cos_sim) for y, cos_sim in similarities)


# def upload(user_similarities):
#     x, similarities = user_similarities
#     rep = '\n'.join(y + ':' + str(cos_sim) for y, cos_sim in similarities)
#     out_file = x + '.txt'
#     with open(out_file, 'w') as out_f:
#         out_f.write(rep)
#     os.popen('gsutil cp ' + out_file + ' ' + gfs_output_path + x + '.txt').read()


def get_files_list():
    if mode == Mode.gfs:
        res = os.popen("gsutil ls " + gfs_input_path).read()
        return list(filter(lambda x: x != '' and x != gfs_input_path, res.split('\n')))
    elif mode == Mode.local:
        return glob.glob(local_input_path + '*.txt')


def build_rdd(files_list):
    rdd = sc.emptyRDD()
    for rating_file in files_list:
        print('Adding {}'.format(rating_file))
        user_ratings = []
        if mode == Mode.gfs:
            file_content = os.popen("gsutil cat " + rating_file).read()
            lines_iterator = filter(lambda x: x != '', file_content.split('\n'))
            user_ratings = [(pair[0], pair[1]) for pair in [line.rstrip().split(',') for line in lines_iterator]]
        elif mode == Mode.local:
            with open(rating_file, 'r') as rating_f:
                user_ratings = [(pair[0], pair[1]) for pair in [line.rstrip().split(',') for line in rating_f]]

        movie_number = filter(str.isdigit, rating_file)
        rdd = rdd.union(sc.parallelize([[movie_number, user_ratings]]))
    return rdd


def main():
    start_time = time.time()
    print('Python cluster version: {}'.format(sys.version))
    print('Current cluster configuration:')
    keys = ['spark.yarn.executor.memoryOverhead', 'spark.yarn.am.memoryOverhead', 'spark.driver.maxResultSize']
    for key in keys:
        print('{}={}'.format(key, sc._conf.get(key)))
    files_list = get_files_list()
    print('Found {} files...'.format(len(files_list)))
    rdd = build_rdd(files_list)
    output_folder = '/tmp/worker_out_' + time.ctime(int(start_time)).replace(' ', '_').replace(':', '-')
    rdd = rdd.flatMap(build_users_pairs).groupByKey().flatMap(cos_sim_and_duplication).groupByKey().flatMap(prettify).saveAsTextFile(output_folder)
    if mode == Mode.gfs:
        print(os.popen('hadoop fs -copyToLocal ' + output_folder + ' ' + output_folder).read())
        print(os.popen('gsutil cp -r ' + output_folder + ' ' + gfs_output_path).read())
    end_time = time.time()
    print("--- %s seconds ---" % (end_time - start_time))


mode = Mode.gfs
sc = pyspark.SparkContext()
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)
gfs_output_path = 'gs://scriba/results/'
gfs_input_path = 'gs://scriba/ratings_by_movie_recent/'
local_output_path = '/home/gianvito/Desktop/Spark/digging_small/out/'
local_input_path = '/home/gianvito/Desktop/Spark/digging_small/'
main()





