
from distutils.core import setup

setup(name = 'lemonade',
      version = '1.0b1',
      description = 'Port of the LEMON Parser Generator',

      scripts = ['bin/lemonade'],
      packages = ['lemonade'],
      package_data = { 'lemonade': ['lempar.tmpl'] },
      
      classifiers = [
          'License :: Public Domain',
          'Development Status :: 4 - Beta',
          'Programming Language :: Python :: 2',
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Code Generators',
          'Topic :: Software Development :: Compilers',
          ],
      
      author = 'Leif Strand',
      author_email = 'leif@cacr.caltech.edu',
      )
