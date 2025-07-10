import pandas as pd

# Daftar path file CSV lokal
urls = [
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_1.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_2.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_3.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_4.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_5.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_6.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_7.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_8.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_9.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_10.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_11.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_1_Pass.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_2_Pass.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_3_Pass.csv",
    r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\SMBM_4_Pass.csv"
]

# Baca semua file ke dalam list DataFrame
dfs = [pd.read_csv(path) for path in urls]

# Gabungkan semua DataFrame
df_combined = pd.concat(dfs, ignore_index=True)

# Simpan ke file CSV gabungan
output_path = r"D:\Semester 7\Skripsi\Data\ArcGIS\git\aoi\Hasil\Dashboard\Dashboard_Posisi\Dashboard_Posisi.csv"
df_combined.to_csv(output_path, index=False)

print("File gabungan berhasil disimpan.")
