# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 08:47:23 2020

@author: blair granville
"""
import pyodbc
import pandas

def catch_error_wrapper(func):
    def class_method_wrapper(self, *args, **kwargs):

        try:
            if args and kwargs:
                return func(self, *args, **kwargs)
            elif kwargs:
                return func(self, **kwargs)
            elif args:
                return func(self, *args)
            else:
                return func(self)
        except Exception as e:
            print('{}() failure: {}'.format( \
                          func.__name__,
                          repr(e)))
            return None
    return class_method_wrapper

class SQLDBConnection():

    def __init__(self, server, readonly=True, query=None):
        self.server = server
        self.readonly = readonly
        self.get_connection()
        self.query = query

    @catch_error_wrapper
    def get_connection(self, server=None):
        if server:
            self.server=server
        self.connection = pyodbc.connect(driver = '{SQL Server Native Client 11.0}',
                              server = self.server,
                              trusted_connection = 'yes',
                              readonly = self.readonly,
                              timeout = 20,
                              autocommit = True)
        self.cursor = self.connection.cursor()

    @catch_error_wrapper
    def change_connection_readonly_state(self):
        self.connection.SQL_ATTR_ACCESS_MODE = False

    @catch_error_wrapper
    def execute_query(self, query=None):
        if query:
            self.query = query
        try:
            self.cursor
        except:
            self.get_connection()
        self.cursor.execute(self.query)

    @catch_error_wrapper
    def execute_query_file(self, file_loc):
        with open(file_loc, 'r') as query_file:
            self.query = query_file.read()
        try:
            self.cursor
        except:
            self.get_connection()
        self.cursor.execute(self.query)

    @catch_error_wrapper
    def commit_query(self):
        self.connection.commit()

    @catch_error_wrapper
    def process_cursor_data(self):
        self.columns = [i[0] for i in self.cursor.description]
        self.data = [[i for i in r] for r in self.cursor.fetchall()]

    @catch_error_wrapper
    def data_to_dataframe(self):
        return pandas.DataFrame(data=self.data, columns=self.columns)

    @catch_error_wrapper
    def data_to_set(self):
        if len(self.columns) > 1:
            raise Exception(TypeError('data_to_set requires a SQL query output with only one column.'))
        else:
            return set([i[0] for i in self.data])

    @catch_error_wrapper
    def execute_query_to_set(self, query):
        self.query = query
        self.execute_query()
        self.process_cursor_data()
        return self.data_to_set()

    @catch_error_wrapper
    def execute_query_to_dataframe(self, query):
        self.query = query
        self.execute_query()
        self.process_cursor_data()
        return self.data_to_dataframe()

    @catch_error_wrapper
    def upload_data(self, data, query):
        try:
            self.cursor
        except:
            self.get_connection()

        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(query, data)
        except Exception as E:
             print(E.args)
             if E.args[1].find('Invalid character value for cast specification') > -1:
                print('During attempt to upload data to database, '
                      'found invalid character value for cast specification '
                      'error due; retrying '
                      'after changing input values to strings.')
                data = [[str(itm) for itm in tup] for tup in data]
                self.cursor.fast_executemany = True
                self.cursor.executemany(query, data)

def close(self):
        try:
            self.connection.close()
        except:
            pass
