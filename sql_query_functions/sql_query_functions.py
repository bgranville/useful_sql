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

    """To create a new instance of this class, you have to provide a server.
    Eg db_connection = sql_query_functions.SQLDBConnection(server='servername')
    The core functions a user would be expected to use have descriptions."""

    def __init__(self, server, readonly=True, query=None):
        self.server = server
        self.readonly = readonly
        self.get_connection()
        self.query = query

    @catch_error_wrapper
    def get_connection(self, server=None):
        """This is preconfigured for my own personal preferences, using
        the appropriate driver, a trusted connection, etc. A user could
        set the object connection property manually to configure these a
        different way, eg:
            SQLDBConnection.connection = pyodbc.connect(...)
        """
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
        """Pass a valid SQL query to this function and it will execute it.
        It doesn't by default return the results of a select statement. If you
        want results from a SELECT statement, it is better to use one of the
        sql_query_functions:
        SQLDBConnection.execute_query_to_set(query)
        SQLDBConnection.execute_query_to_dataframe(query)

        but you can then get results after execute_query from the object via:
        To get headers and a list of lists:
            SQLDBConnection.process_cursor_data()
            headers = SQLDBConnection.columns
            list_of_lists = SQLDBConnection.data
        To get a dataframe (pandas)
            dataframe = SQLDBConnection.data_to_dataframe()
        To get a single column as a set:
            dataset = SQLDBConnection.data_to_set()
        """
        if query:
            self.query = query
        try:
            self.cursor
        except:
            self.get_connection()
        self.cursor.execute(self.query)

    @catch_error_wrapper
    def execute_query_file(self, file_loc):
        """Specify a .sql file location, and then use this function to execute it.
        Will not return any data - just executes the query! You can get the data
        via the same instructions for  the execute_query function."""
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
        """Pass a select query that returns a single column, and this will
        return those results as a set."""
        self.query = query
        self.execute_query()
        self.process_cursor_data()
        return self.data_to_set()

    @catch_error_wrapper
    def execute_query_to_dataframe(self, query):
        """Pass a select query that returns multiple columns, and this will
        return those results as a pandas dataframe."""
        self.query = query
        self.execute_query()
        self.process_cursor_data()
        return self.data_to_dataframe()

    @catch_error_wrapper
    def upload_data(self, data, query):
        """Pass this function a list of tuples and an appropriately
        formatted query and it will upload the data.
        Note that the user needs permission to write to the database,
        and needs to have set the initial SQLDBConnection as readonly=False.
        A pandas dataframe can be turned into a list of tuples via:
            list_of_tuples = list(dataframe.itertuples(index=False, name=None))
        And the INSERT query should be formatted:
            query = 'INSERT INTO {table_name} ({list_of_columns}) VALUES ({question_marks})'
            where:
                table_name = a string that is the name of the table, eg 'mdw.dbo.test_table'
                list_of_columns = comma separated list of columns in the dataframe, eg 'id,first_name,last_name,age'
                question_marks = comma separated question marks; the same number as there are columns, eg '?,?,?,?'
        """
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

    @catch_error_wrapper
    def upload_dataframe(self, dataframe, full_table_name):
        """This is a function to upload a dataframe - slightly simpler than the
        upload_data function as it does some of the work for you.
        Pass it the dataframe and the full table name - note it will create and
        write the table, so it will fail if the table already exists!
        """
        data = list(dataframe.itertuples(index=False, name=None))
        list_of_columns = ', '.join(dataframe.columns)
        question_marks = ', '.join(['?' for i in dataframe.columns])
        query = f'INSERT INTO {full_table_name} ({list_of_columns}) VALUES ({question_marks})'

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
