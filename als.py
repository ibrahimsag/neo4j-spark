from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS


def append_to_file(file_path, lines):
    with open(file_path, 'a') as f:
        for line in lines:
            f.write('\n' + line)


if __name__ == "__main__":
    sc = SparkContext(appName="RunASL")

    ratings_file = sc.textFile("ds/dumped-ratings.csv")
    ratings_head = ratings_file.first()
    ratings_records = ratings_file.filter(
        lambda l: l != ratings_head
    ).map(
        lambda line: [s.strip('"') for s in line.split(",")]
    )

    isbns = ratings_records.map(
        lambda record: record[1]
    ).distinct().zipWithUniqueId()
    isbn_ids = isbns.collect()
    isbn_map = sc.broadcast(dict((isbn, int_id) for isbn, int_id in isbn_ids))
    isbn_map_reverse = sc.broadcast(dict((int_id, isbn) for isbn, int_id in isbn_ids))
    ratings = ratings_records.map(
        lambda record: (int(record[0]), isbn_map.value.get(record[1], record[1]), float(record[2]))
    )

    user_ids = ratings.map(lambda record: record[0]).distinct().take(1000)

    model = ALS.trainImplicit(ratings, 10, 10, 0.01)
    get_5_recommends = lambda user_id: model.recommendProducts(user_id, 5)
    def clear_for_dump(rating):
        return '|'.join(
            [
                isbn_map_reverse.value.get(rating.product, "MISSING"),
                '"{}"'.format(rating.user),
                'Recommended',
                "{0:02f}".format(rating.rating)])

    recommendations = []
    for user_id in user_ids:
        recommendations.extend([clear_for_dump(rat) for rat in get_5_recommends(user_id)
                                if rat.rating > 0])

    with open('ds/recommendations.csv', 'w') as f:
        f.write("|".join([":START_ID(Book)", ":END_ID(User)", ":TYPE", "coeff"]))
    append_to_file('ds/recommendations.csv', recommendations)

    sc.stop()

