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

VERSION = "0.5.4"

version_path = os.path.join(os.path.dirname(__file__), 'lib', 'chorde', '_version.py')
if not os.path.exists(version_path):
    with open(version_path, "w") as version_file:
        pass
with open(version_path, "r+") as version_file:
    version_content = "__version__ = %r" % (VERSION,)
    if version_file.read() != version_content:
        version_file.seek(0)
        version_file.write(version_content)
        version_file.flush()
        version_file.truncate()

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme_file:
    readme = readme_file.read()

import re
import os.path
_extras_requirements = [
    ( re.compile(r'pyzmq'), ['mq'] ),
    ( re.compile(r'numpy'), ['shmem'] ),
    ( re.compile(r'memcache'), ['memcache','elasticache'] ),
    ( re.compile(r'dnspython'), ['memcache','elasticache'] ),
]
with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as requirements_file:
    all_requirements = list(filter(bool, [ r.strip() for r in requirements_file ]))
# Compute extras_requires and main requirements
requirements = []
extras_requirements = {}
for req in all_requirements:
    isbase = True
    for pat, extnames in _extras_requirements:
        if pat.match(req):
            isbase = False
            for extname in extnames:
                extras_requirements.setdefault(extname, []).append(req)
    if isbase:
        requirements.append(req)

extra = {}

packages = [
    "chorde",
    "chorde.clients",
    "chorde.mq",
    "chorde.mq.ipsub",
]

if not no_pyrex:
    basedir = os.path.dirname(__file__)
    libdir = os.path.join(basedir, 'lib')
    extra.update(dict(
        ext_modules=cythonize([
            Extension("chorde.lrucache", ["lib/lrucache/lrucache.pyx"],
                depends = ["lib/lrucache/lrucache.pxd", "lib/lrucache/lrucache_cy028_compat.h"],
                cython_include_dirs = [libdir, os.path.join(libdir, "lrucache")],
                extra_compile_args = [ "-O3" ] ),
            Extension("chorde.cuckoocache", ["lib/cuckoocache/cuckoocache.pyx"],
                depends = ["lib/cuckoocache/cuckoocache.pxd"],
                cython_include_dirs = [libdir, os.path.join(libdir, "cuckoocache")],
                extra_compile_args = [ "-O3" ] ),
            Extension("chorde.clients._async", ["lib/chorde/clients/_async.pyx"],
                depends = ["lib/chorde/clients/_async.pxd"],
                cython_include_dirs = [libdir, os.path.join(libdir, "chorde", "clients")],
                extra_compile_args = [ "-O3" ] ),
            Extension("chorde.decorators", ["lib/chorde/decorators.py"]),
            Extension("chorde.clients.base", ["lib/chorde/clients/base.py"]),
            Extension("chorde.clients.inproc", ["lib/chorde/clients/inproc.py"]),
            Extension("chorde.clients.tiered", ["lib/chorde/clients/tiered.py"]),
            Extension("chorde.clients.async", ["lib/chorde/clients/async.py"]),
        ], include_path = [ libdir ]),
        cmdclass = {'build_ext': build_ext},
        package_data = {
            'chorde' : ['lrucache.pxd', 'cuckoocache.pxd'],
            'chorde.clients' : ['_async.pxd'],
        },
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

    install_requires = requirements,
    extras_require = extras_requirements,

    classifiers=[
        "Development Status :: 4 - Beta",
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

