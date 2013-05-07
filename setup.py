from distutils.core import setup
from distutils.extension import Extension
from Pyrex.Distutils import build_ext
import os.path

VERSION = "0.1"

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme_file:
    readme = readme_file.read()

setup(
  name = "chorde",
  version = VERSION,
  description = "Clustered Caching Library",
  author = "Claudio Freire",
  author_email = "klaussfreire@gmail.com",
  url = "https://bitbucket.org/claudiofreire/chorde/",
  license = "LGPLv3",
  long_description = readme,
  
  packages = ["chorde"],
  package_dir = {'':'lib'},
  
  ext_modules=[ 
    Extension("chorde.lrucache", ["lib/lrucache/lrucache.pyx"],
              extra_compile_args = [ "-O3" ] )
              #extra_compile_args = ["-march=pentium4","-mfpmath=sse","-msse2"] )
    ],
  cmdclass = {'build_ext': build_ext}
)
