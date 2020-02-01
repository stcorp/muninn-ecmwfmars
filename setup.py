from setuptools import setup

setup(
    name='muninn-ecmwfmars',
    version='1.0',
    author="S[&]T",
    url="https://github.com/stcorp/muninn-ecmwfmars",
    description="Muninn extension for ECMWF GRIB products from the mars archive",
    license="BSD",
    py_modules=['muninn_ecmwfmars'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
    ],
    install_requires=[
        "muninn",
    ]
)
