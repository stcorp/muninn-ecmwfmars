from setuptools import setup

setup(
    name="muninn-ecmwfmars",
    version="1.1",
    description="Muninn extension for ECMWF GRIB products from the mars archive",
    url="https://github.com/stcorp/muninn-ecmwfmars",
    author="S[&]T",
    license="BSD",
    py_modules=["muninn_ecmwfmars"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
        "Environment :: Plugins",
    ],
    install_requires=["muninn"]
)
