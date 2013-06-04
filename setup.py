try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

try:
    from Pyrex.Distutils import build_ext
    no_pyrex = False
except:
    no_pyrex = True
import os.path

VERSION = "0.1"

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme_file:
    readme = readme_file.read()

extra = {}

packages = [
      "chorde",
      "chorde.clients",
]

if not no_pyrex:
    extra.update(dict(
        ext_modules=[ 
          Extension("chorde.lrucache", ["lib/lrucache/lrucache.pyx"],
                    extra_compile_args = [ "-O3" ] )
                    #extra_compile_args = ["-march=pentium4","-mfpmath=sse","-msse2"] )
          ],
        cmdclass = {'build_ext': build_ext}
    ))
    packages.append("chorde.mq")

setup(
  name = "chorde",
  version = VERSION,
  description = "Clustered Caching Library",
  author = "Claudio Freire",
  author_email = "klaussfreire@gmail.com",
  url = "https://bitbucket.org/claudiofreire/chorde/",
  license = "LGPLv3",
  long_description = readme,
  packages = packages,
  package_dir = {'':'lib'},
  
  tests_require = ['nose'],
  test_suite = 'tests',
  
  classifiers=[
            "Development Status :: 1 - Planning",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
            "Programming Language :: Python",
            "Programming Language :: Cython",
            "Programming Language :: Python :: 2",
            "Topic :: Software Development :: Libraries",
            "Operating System :: OS Independent",
            ],
  **extra
)

