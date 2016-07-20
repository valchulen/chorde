import sys
import os.path

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

if sys.subversion[0] == 'PyPy': 
    # Even though pypy may have Pyrex or Cython, cython LRU isn't compatible with cpyext
    no_pyrex = True
else:
    no_pyrex = False
    try:
        from Cython.Distutils import build_ext, Extension
        from Cython.Build import cythonize
    except:
        no_pyrex = True

VERSION = "0.1"

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme_file:
    readme = readme_file.read()

extra = {}

packages = [
      "chorde",
      "chorde.clients",
      "chorde.mq",
      "chorde.mq.ipsub",
]

if not no_pyrex:
    extra.update(dict(
        ext_modules=cythonize([ 
          Extension("chorde.lrucache", ["lib/lrucache/lrucache.pyx"],
                    depends = ["lib/lrucache/lrucache.pxd"],
                    cython_include_dirs = [os.path.join(os.path.dirname(__file__), "lib/lrucache")],
                    extra_compile_args = [ "-O3" ] ),
                    #extra_compile_args = ["-march=pentium4","-mfpmath=sse","-msse2"] ),
          Extension("chorde.clients._async", ["lib/chorde/clients/_async.pyx"],
                    depends = ["lib/chorde/clients/_async.pxd"],
                    extra_compile_args = [ "-O3" ] ),
                    #extra_compile_args = ["-march=pentium4","-mfpmath=sse","-msse2"] ),
          ]),
        cmdclass = {'build_ext': build_ext},
        data_files = [
            ('chorde', ['lib/lrucache/lrucache.pxd']),
            ('chorde/clients', ['lib/chorde/clients/_async.pxd'])
        ],
    ))

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
  
  tests_require = 'nose',
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
  zip_safe = False,
  **extra
)

