from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

DELIMITER = '|'
class BXData:
    pass

class UsersFile(BXData):
    file_path = '/Users/ibrahims/study/books/ds/BX-Users.csv'
    output_file = '/Users/ibrahims/study/books/ds/users.csv'
    columns = [
        {
            'label': 'id:ID(User)',
            'get': lambda t: t[0]
        },
        {
            'label': ':LABEL',
            'get': lambda t: 'User'
        },
        {
            'label': 'location',
            'get': lambda t: t[1]
        },
        {
            'label': 'age',
            'get': lambda t: t[2]
        }
    ]


class BooksFile(BXData):
    file_path = '/Users/ibrahims/study/books/ds/BX-Books.csv'
    output_file = '/Users/ibrahims/study/books/ds/books.csv'
    columns = [
        { 'label': 'id:ID(Book)',
          'get': lambda t: t[0]
        },
        { 'label': ':LABEL',
          'get': lambda t: 'Book'
        },
        { 'label': 'title',
          'get': lambda t: t[1]
        },
        { 'label': 'author',
          'get': lambda t: t[2]
        },
        { 'label': 'pubyear',
          'get': lambda t: t[3]
        },
        { 'label': 'publisher',
          'get': lambda t: t[4]
        }
    ]


class RatingsFile(BXData):
    file_path = '/Users/ibrahims/study/books/ds/BX-Book-Ratings.csv'
    output_file = '/Users/ibrahims/study/books/ds/ratings.csv'
    columns = [
        { 'label': ':START_ID(User)',
          'get': lambda t: t[0]
        },
        { 'label': ':END_ID(Book)',
          'get': lambda t: t[1]
        },
        { 'label': ':TYPE',
          'get': lambda t: 'Rated'
        },
        { 'label': 'rating',
          'get': lambda t: t[2]
        },
    ]


def clear_null(line):
    return line.replace(";NULL", ';"0"')


def clear_delimiter(line):
    return line.replace(DELIMITER, '')


def clear_separation(line):
    return '"|"'.join(line.split('";"'))


def prepare_file(data_file):
    def prepare_line(line):
        clear_line = clear_null(clear_delimiter(line))
        escaped_line = clear_separation(clear_line)
        props = escaped_line.split("|")
        return props
    return prepare_line


def prepare_for_csv(data_file):
    def prepare_props(props):
        processed_line = DELIMITER.join([c['get'](props) for c in data_file.columns])
        return processed_line
    return prepare_props


def get_records_rdd(sc, data_file):
    print("reading ", data_file.file_path)
    input_file = sc.textFile(data_file.file_path)
    print("getting header ", data_file.file_path)
    header_line = input_file.first()
    print("clearing ", data_file.file_path)
    return (
        input_file
        .filter(lambda l: l != header_line)
        .map(prepare_file(data_file))
    )


def process_file(sc, data_file):
    records_rdd = get_records_rdd(sc, data_file)
    clear_file = records_rdd.map(prepare_for_csv(data_file))
    with open(data_file.output_file, 'w') as f:
        f.write(DELIMITER.join([c['label'] for c in data_file.columns]))

    lines = clear_file.collect()
    append_to_file(data_file.output_file, lines)
    

def append_to_file(file_path, lines):
    with open(file_path, 'a') as f:
        for line in lines:
            f.write('\n' + line.encode('utf8'))


def process_files(sc):
    process_file(sc, BooksFile())
    process_file(sc, UsersFile())
    process_file(sc, RatingsFile())


def add_missing_isbns(sc):
    # prepare (isbn, title) pairs
    book_records = get_records_rdd(sc, BooksFile)
    isbn_titles = book_records.map(lambda l: l[:2])
    # prepare (isbn, userid) pairs
    rating_records = get_records_rdd(sc, RatingsFile)
    rating_pairs = rating_records.map(lambda l: (l[1], l[0]))

    # join pair rdds to get nonexistent isbns
    missing_isbns = (
        rating_pairs
        .leftOuterJoin(isbn_titles)
        # .map(lambda l: l[1])
        # .take(5)
        .filter(lambda l: not l[1][1])
        .map(lambda l: (l[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .map(lambda l: l[0])
        .map(lambda l: (l, '"empty title"', '"unknown author"', '"unknown year"', '"unknown publisher"'))
        .map(prepare_for_csv(BooksFile))
        .collect()
    )
    append_to_file(BooksFile.output_file, missing_isbns)


if __name__ == "__main__":
    sc = SparkContext(appName="ClearCSV")

    process_files(sc)
    add_missing_isbns(sc)
    
    sc.stop()
