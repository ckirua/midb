import setuptools
from Cython.Build import cythonize
from setuptools import Extension, find_packages

LANGUAGE_LEVEL = 3

extensions = [
    Extension("midb.postgres.*", ["midb/postgres/*.pyx"]),
]

setuptools.setup(
    name="midb",
    packages=["midb"] + [f"midb.{pkg}" for pkg in find_packages(where="midb")],
    ext_modules=(
        cythonize(extensions, build_dir="src", language_level=LANGUAGE_LEVEL)
        if extensions
        else []
    ),
)
