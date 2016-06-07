try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

setup(name='peloton_bloomfilters',
      author = 'Adam DePrince',
      author_email = 'adam@pelotoncycle.com',
      url = 'https://github.com/pelotoncycle/peloton_bloomfilters',
      version='0.0.1',
      description="Peloton Cycle's Bloomin fast Bloomfilters",
      ext_modules=(
          [
              Extension(
                  name='peloton_bloomfilters',
                  sources=['peloton_bloomfiltersmodule.c']),
          ]
      )
)
