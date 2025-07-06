from setuptools import setup, find_packages

setup(
    name="aisindonesia",
    version="0.1.0",
    description="Utilities for working with Indonesian AIS and geospatial H3 data",
    author="Gery Nastiar",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "pytest",
        "geopandas",
        "shapely",
        "folium",
        "h3",
        "sedona",
        "apache-sedona",
        "spark"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
