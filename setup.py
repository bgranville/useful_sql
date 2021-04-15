from setuptools import setup

setup(
   name='useful_sql',
   version='1.0',
   description='My personal module for doing some common tasks with PYODBC',
   author='Blair Granville',
   author_email='blair.granville+github@gmail.com',
   packages=['useful_sql'],  #same as name
   install_requires=['pyodbc', 'pandas'], #external packages as dependencies
)
