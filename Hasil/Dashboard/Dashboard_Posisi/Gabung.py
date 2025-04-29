import pandas as pd

# Daftar URL file CSV
urls = [
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_1.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_2.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_3.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_4.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_5.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_6.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_7.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_8.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_9.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_10.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_11.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_1_Pass.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_2_Pass.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_3_Pass.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/Dashboard/Dashboard_Posisi/SMBM_4_Pass.csv",
]


# Baca semua file ke dalam list DataFrame
dfs = [pd.read_csv(url) for url in urls]

# Gabungkan semua DataFrame
df_combined = pd.concat(dfs, ignore_index=True)

# Pastikan kolom diff_time_hout hanya berisi angka
df_combined['diff_time'] = pd.to_numeric(df_combined['diff_time'], errors='coerce')

# Hapus baris yang NaN (bukan angka)
df_combined = df_combined.dropna(subset=['diff_time'])

# Simpan ke file CSV
df_combined.to_csv("Dashboard_Posisi.csv", index=False)

print("sudah")
