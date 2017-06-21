import csv
import os

import luigi
import luigi.contrib.s3
import luigi.contrib.postgres

import psycopg2
import pdb


# Configuration classes
class postgresTable(luigi.Config):
    host = luigi.Parameter()
    password = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()

class QueryPostgres:
    def rows(self):
        connection = psycopg2.connect(
            host=postgresTable().host,
            port=5432,
            database=postgresTable().database,
            user=postgresTable().user)
        connection.set_client_encoding('utf-8')

        cursor = connection.cursor()
        sql = 'select * from user_updates limit 1'
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

class WriteUserUpdatesToSQL(luigi.contrib.postgres.CopyToTable):
    id = luigi.Parameter()
    host = postgresTable().host
    password = postgresTable().password
    database = postgresTable().database
    user = postgresTable().user
    table = 'user_updates'

    @property
    def update_id(self):
        return '{}_{}'.format(self.table, self.id)

    columns = [
        ('ts', 'timestamp'),
        ('user_id', 'int'),
        ('email', 'text'),
        ('name', 'text'),
        ('environment', 'text'),
        ('other_data', 'text')
    ]

    @property
    def source_csv(self):
        return './generated_files/{}_user_updates.csv'.format(self.id)

    def rows(self):
        return QueryPostgres().rows()

if __name__ == '__main__':
    luigi.run()
