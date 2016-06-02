try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

setup(name='shared_memory_bloomfilter',
      version='0.0.1',
      description='Peloton modules',
      ext_modules=(
          [
              Extension(
                  name='shared_memory_bloomfilter',
                  sources=['shared_memory_bloomfiltermodule.c']),
          ]
      )
)
