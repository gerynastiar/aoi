from pyspark.sql import SparkSession
import geopandas as gpd # membuat geodataframe
import pandas as pd # membuat dataframe pandas
import h3 # membuat dan membantu visualisasi index h3

import matplotlib # plotting untuk visualisasi data
import matplotlib.pyplot as plt # modul dalam matplotlib untuk membuat plot dan grafik
from shapely.geometry import Polygon # kelas Shapely untuk membuat dan memanipulasi poligon
from datetime import datetime # modul untuk manipulasi tanggal dan waktu
import geopandas as gpd
import folium
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union
import h3

# SEDONA: memungkinkan penggunaan query SQL untuk memproses dan menganalisis data spasial.
import sedona.sql # modul untuk menjalankan query SQL pada data spasial
from sedona.register import SedonaRegistrator # alat untuk mendaftarkan Sedona ke Spark
from sedona.utils import SedonaKryoRegistrator, KryoSerializer 
# registrator untuk serialisasi objek spasial dengan Kryo
# serializer untuk meningkatkan kinerja serialisasi

# PYSPARK: antarmuka Python untuk Apache Spark
import pyspark.sql.functions as F # modul untuk fungsi SQL pada DataFrame
import pyspark.sql.types as pst # modul untuk tipe data SQL pada DataFrame
from pyspark import StorageLevel # kelas untuk menentukan tingkat penyimpanan RDD
from pyspark.sql import SparkSession  # kelas untuk membuat dan mengelola sesi Spark

import h3

import sys
import subprocess

# from ais import functions as af
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import h3.api.numpy_int as h3int
from shapely.geometry import mapping, Polygon, Point

from multiprocessing import Pool
import tqdm

import geopandas as gpd
import pandas as pd
import numpy as np

import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
import h3
import json

import geopandas as gpd
import h3
import folium
from shapely.geometry import Polygon, mapping
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
from pyspark.sql.functions import concat_ws, split, lit, min, max
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.window import Window

from shapely.geometry import Point, Polygon, mapping
from IPython.display import HTML
from multiprocessing import Pool

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, countDistinct, when, expr, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second
from pyspark.sql.functions import monotonically_increasing_id, lead, lag, abs, row_number
from pyspark.sql.functions import concat_ws, split, lit, min, max
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql.window import Window

from shapely.geometry import Point, Polygon, mapping
from IPython.display import HTML
from multiprocessing import Pool
from pyspark.sql import SparkSession

def clean_column(df):
    return df.dropna()


def read_aoi(approach: str) -> gpd.GeoDataFrame:
    """
    Membaca shapefile AOI berdasarkan pendekatan yang dipilih.

    Parameters:
        approach (str): 'Manual', 'Cluster', 'Distance', atau 'Heatmap'.

    Returns:
        GeoDataFrame
    """
    base_url = "https://raw.githubusercontent.com/gerynastiar/aoi/main"
    file_map = {
        "Manual": "Manual/Final/DLKr_Indo_Final.shp",
        "Cluster": "Cluster/Cluster_AOI_Final.shp",
        "Distance": "Distance/Distance%20Fix%20135/Distance_AOI_135_Ports_Final.shp",
        "Heatmap": "Heatmap/Shapefile/Gabungan_Heatmap/Gabungan.shp"
    }

    if approach not in file_map:
        raise ValueError(f"Approach '{approach}' tidak dikenali. Pilih salah satu dari: {list(file_map.keys())}")

    full_url = f"/vsicurl/{base_url}/{file_map[approach]}"
    
    try:
        gdf = gpd.read_file(full_url)
        gdf = gdf.to_crs("EPSG:4326")  # Pastikan WGS 84
        print(f"✅ Berhasil membaca AOI '{approach}' dengan {len(gdf)} fitur.")
        return gdf
    except Exception as e:
        print(f"❌ Gagal membaca shapefile: {e}")
        return gpd.GeoDataFrame()


def polygon_to_h3(polygon, resolution=8):
    """
    Mengubah satu polygon (Shapely) menjadi daftar index H3.

    Parameters:
        polygon (Polygon or MultiPolygon)
        resolution (int): Resolusi H3

    Returns:
        list: Daftar hex_id H3
    """
    if not polygon.is_valid:
        return []
    if polygon.geom_type == 'MultiPolygon':
        polygon = polygon.geoms[0]
    if polygon.exterior is None:
        return []

    try:
        coords = [(y, x) for x, y in polygon.exterior.coords]  # lat, lng
        hexagons = h3.polyfill({"type": "Polygon", "coordinates": [coords]}, resolution)
        return list(hexagons)
    except Exception as e:
        print(f"Error processing polygon: {e}")
        return []


def add_h3_column(gdf: gpd.GeoDataFrame, resolution=8) -> gpd.GeoDataFrame:
    """
    Tambahkan kolom 'h3_ids' ke GeoDataFrame hasil AOI.

    Parameters:
        gdf (GeoDataFrame): GeoDataFrame dengan kolom 'geometry'
        resolution (int): Resolusi H3

    Returns:
        GeoDataFrame: dengan kolom baru 'h3_ids'
    """
    gdf["h3_ids"] = gdf["geometry"].apply(lambda geom: polygon_to_h3(geom, resolution))
    return gdf


def plot_h3_on_map(gdf: gpd.GeoDataFrame, center=[-8.743413, 115.209843], zoom_start=14, color='green'):
    """
    Buat peta Folium dari kolom h3_ids di GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): harus mengandung kolom 'h3_ids'
        center (list): koordinat awal peta
        zoom_start (int): tingkat zoom awal
        color (str): warna fill hexagon

    Returns:
        folium.Map
    """
    m = folium.Map(location=center, zoom_start=zoom_start)

    for idx, row in gdf.iterrows():
        for hex_id in row.get("h3_ids", []):
            boundary = h3.h3_to_geo_boundary(hex_id, geo_json=True)
            hex_poly = Polygon(boundary)
            folium.Polygon(
                locations=[[lat, lng] for lat, lng in hex_poly.exterior.coords],
                color='gray',
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.4,
            ).add_to(m)
    return m

def explode_h3(hexagons: pd.DataFrame, spark: SparkSession):
    """
    Meledakkan kolom H3 ID dari GeoDataFrame dan ubah ke DataFrame Spark.

    Parameters:
    -----------
    hexagons : pd.DataFrame
        DataFrame dengan kolom 'h3_ids' berisi list H3 string.
    spark : SparkSession
        Objek SparkSession aktif.

    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame Spark hasil ledakan H3
    """
    if "h3_ids" not in hexagons.columns:
        raise ValueError("Kolom 'h3_ids' tidak ditemukan di DataFrame")

    hexagons = hexagons.copy()
    hexagons["h3_ids_int"] = hexagons["h3_ids"].apply(lambda x: [int(h, 16) for h in x])
    exploded = hexagons.explode("h3_ids_int").drop(columns=["geometry"], errors="ignore")
    spark_df = spark.createDataFrame(exploded)
    return spark_df

