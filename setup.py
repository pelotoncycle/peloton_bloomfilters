try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

setup(name='peloton_bloomfilter',
      version='0.0.1',
      description='Peloton Bloomfilter',
      ext_modules=(
          [
              Extension(
                  name='peloton_bloomfilter',
                  sources=['peloton_bloomfiltermodule.c']),
          ]
      )
)
