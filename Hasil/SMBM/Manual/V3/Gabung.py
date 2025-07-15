import pandas as pd

# Daftar URL file CSV
urls = [
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_1.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_2.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_3.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_4.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_5.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_6.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_7.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_8.csv",
    "https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_9.csv"
]



# Baca semua file ke dalam list DataFrame
dfs = [pd.read_csv(url) for url in urls]

# Gabungkan semua DataFrame
df_combined = pd.concat(dfs, ignore_index=True)

# df_combined['time'] = pd.to_numeric(df_combined['time'], errors='coerce')

# df_combined = df_combined.dropna(subset=['time'])

# Simpan ke file CSV
df_combined.to_csv("SMBM_Manual_All.csv", index=False)

print("sudah")

# import requests
# import os

# def split_csv_from_url(url, output_prefix, max_kib=2000):
#     max_bytes = max_kib * 1024  # Konversi KiB ke byte
#     response = requests.get(url)
#     response.raise_for_status()

#     lines = response.text.splitlines()
#     header = lines[0]
#     data_lines = lines[1:]

#     file_count = 1
#     current_bytes = len((header + '\n').encode('utf-8'))

#     output_file = open(f"{output_prefix}_{file_count}.csv", "w", encoding="utf-8")
#     output_file.write(header + '\n')

#     for line in data_lines:
#         encoded_line = (line + '\n').encode('utf-8')
#         if current_bytes + len(encoded_line) > max_bytes:
#             output_file.close()
#             file_count += 1
#             output_file = open(f"{output_prefix}_{file_count}.csv", "w", encoding="utf-8")
#             output_file.write(header + '\n')
#             current_bytes = len((header + '\n').encode('utf-8'))

#         output_file.write(line + '\n')
#         current_bytes += len(encoded_line)

#     output_file.close()
#     print(f"Selesai! Terbagi menjadi {file_count} file dengan maksimal {max_kib} KiB.")

# # Contoh penggunaan
# split_csv_from_url(
#     url="https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_All.csv",
#     output_prefix="SMBM_Split",
#     max_kib=2000  # 2000 KiB = 2 MiB
# )

# import requests
# import os
# import csv
# import io

# def split_csv_filtered_by_vessel_type(url, output_prefix, max_kib=2000):
#     max_bytes = max_kib * 1024  # Konversi ke byte
#     response = requests.get(url)
#     response.raise_for_status()

#     csv_text = response.text
#     csv_file = io.StringIO(csv_text)
#     reader = csv.reader(csv_file)

#     header = next(reader)
#     vessel_type_idx = header.index("vessel_type")
#     port_idx = header.index("Kode")

#     # Filter data
#     allowed_ports = {"TDA", "GRE", "SUB", "SRG", "SOQ", "DUM"}
#     filtered_rows = [
#         row for row in reader
#         if row[vessel_type_idx] != "Passenger" and row[port_idx] in allowed_ports
#     ]

#     file_count = 1
#     current_bytes = len(','.join(header).encode('utf-8')) + 1

#     output_file = open(f"{output_prefix}_{file_count}.csv", "w", newline='', encoding="utf-8")
#     writer = csv.writer(output_file)
#     writer.writerow(header)

#     for row in filtered_rows:
#         row_bytes = len(','.join(row).encode('utf-8')) + 1  # Tambah newline
#         if current_bytes + row_bytes > max_bytes:
#             output_file.close()
#             file_count += 1
#             output_file = open(f"{output_prefix}_{file_count}.csv", "w", newline='', encoding="utf-8")
#             writer = csv.writer(output_file)
#             writer.writerow(header)
#             current_bytes = len(','.join(header).encode('utf-8')) + 1

#         writer.writerow(row)
#         current_bytes += row_bytes

#     output_file.close()
#     print(f"Selesai! File dibagi menjadi {file_count} file (tanpa Passenger).")

# # Jalankan fungsi
# split_csv_filtered_by_vessel_type(
#     url="https://raw.githubusercontent.com/gerynastiar/aoi/main/Hasil/SMBM/Manual/V2/SMBM_Manual_All.csv",
#     output_prefix="SMBM_Filtered_Split",
#     max_kib=2000
# )
