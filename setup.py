from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))


setup(name='yaya_quant',
      version='0.1',

      description='Package for quant',

      # URL
      url="#",

      # Author
      author="LH.Li",
      author_email='lh98lee@zju.edu.cn',

      license='#',

      # numpy_ext0.9.8 is needed and rely on python=3.10.9, numpy=1.23.1
      packages=find_packages(),
      )


# python3 setup.py install --u
