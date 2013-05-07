from distutils.core import setup
from distutils.extension import Extension
from Pyrex.Distutils import build_ext

setup(
  name = "chorde",
  ext_modules=[ 
    Extension("chorde.lrucache", ["lib/lrucache/lrucache.pyx"],
              extra_compile_args = [ "-O3" ] )
              #extra_compile_args = ["-march=pentium4","-mfpmath=sse","-msse2"] )
    ],
  cmdclass = {'build_ext': build_ext}
)
