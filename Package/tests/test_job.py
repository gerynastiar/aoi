from aisindonesia.utils import clean_column
from pyspark.sql import SparkSession
import pytest
from aisindonesia.utils import read_aoi, polygon_to_h3, add_h3_column, plot_h3_on_map, explode_h3
from shapely.geometry import Polygon

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("TestExplodeH3").getOrCreate()
    yield spark
    spark.stop()

def test_read_aoi_manual():
    gdf = read_aoi("Manual")
    assert not gdf.empty
    assert "geometry" in gdf.columns

def test_polygon_to_h3_valid():
    poly = Polygon([(115.2, -8.7), (115.2, -8.6), (115.3, -8.6), (115.3, -8.7), (115.2, -8.7)])
    hexes = polygon_to_h3(poly, resolution=8)
    assert isinstance(hexes, list)
    assert all(isinstance(h, str) for h in hexes)

def test_polygon_to_h3_invalid():
    poly = Polygon()
    hexes = polygon_to_h3(poly)
    assert hexes == []

def test_add_h3_column():
    gdf = read_aoi("Manual")
    gdf_h3 = add_h3_column(gdf, resolution=7)
    assert "h3_ids" in gdf_h3.columns
    assert isinstance(gdf_h3.iloc[0]["h3_ids"], list)

def test_plot_h3_on_map_runs():
    gdf = read_aoi("Manual")
    gdf = add_h3_column(gdf, resolution=8)
    m = plot_h3_on_map(gdf)
    assert m is not None

def test_explode_h3(spark):
    gdf = read_aoi("Distance")
    gdf = add_h3_column(gdf, resolution=8)
    hex = explode_h3(gdf, spark)
    assert hex == []