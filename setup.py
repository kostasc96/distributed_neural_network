from setuptools import setup, find_packages, Extension
import pybind11
from Cython.Build import cythonize
import os
import numpy

src_packages = find_packages(where="pcomp_utils")
REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

ext_modules = [
    Extension(
        "pcomp.fast_vector_cpp",  # this places it inside pcomp
        ["pcomp_utils/fast_vector_cpp.cpp"],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=["-O3", "-march=native"],
    ),
     Extension(
        "pcomp.dotmodule",
        ["pcomp_utils/dotmodule.cpp"],
        include_dirs=[
            pybind11.get_include(),
            numpy.get_include(),
        ],
        language="c++",
        extra_compile_args=[
           "-O3",
           "-std=c++14",
           "-march=native", 
           "-mfma",            # for AVX2 FMA
           "-fopenmp",         # enable OpenMP pragmas
        ],
       extra_link_args=[
           "-fopenmp",         # link with OpenMP runtime
       ],
    )
]
ext_modules += cythonize(
    Extension(
        "pcomp.fast_queue",
        ["pcomp_utils/fast_queue.pyx"]
    ),
    compiler_directives={"language_level": "3"}
)
ext_modules += cythonize(
    Extension(
        "pcomp.parser",
        ["pcomp_utils/parser.pyx"]
    ),
    compiler_directives={"language_level": "3"}
)
ext_modules += cythonize(
    Extension(
        "pcomp.fast_vector",
        ["pcomp_utils/fast_vector.pyx"],
        include_dirs=[numpy.get_include()],
    ),
    compiler_directives={"language_level": "3"}
)


setup(
    name="pcomp", #package_name
    version="0.1",
    author="pcomp",
    author_email=None,
    description="Package containining utils for pcomp",
    long_description="This package contains the functions that will be used for pcomp",
    long_description_content_type="text/markdown",
    packages = [f"pcomp.{pkg}" for pkg in src_packages] + ["pcomp"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    include_package_data=True,
    install_requires=REQUIREMENTS,
    package_dir={"pcomp": "pcomp_utils"},  #top-level folder -> pcomp (mapping to pcomp_utils)
    package_data={"pcomp": ["*"]},
    ext_modules=ext_modules
)