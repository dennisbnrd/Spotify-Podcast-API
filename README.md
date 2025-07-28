# ETL Pipeline untuk Podcast Spotify

Proyek ini mengimplementasikan **ETL (Extract, Transform, Load)** pipeline untuk mengambil data podcast dari Spotify API, memprosesnya, dan mengunggahnya ke **Google BigQuery**. Pipeline ini menggunakan **Prefect** untuk workflow orchestration, **GitHub** sebagai sumber kode, dan **GCP** untuk penyimpanan data.

## Daftar Isi

1. [Tujuan Proyek](#tujuan-proyek)
2. [Langkah-Langkah Proyek](#langkah-langkah-proyek)
   - [Extract: Mengambil Data Podcast](#extract-mengambil-data-podcast)
   - [Transform: Pembersihan dan Transformasi Data](#transform-pembersihan-dan-transformasi-data)
   - [Load: Mengunggah Data ke BigQuery](#load-mengunggah-data-ke-bigquery)
   - [Menjalankan Pipeline](#menjalankan-pipeline)
   - [Deployment ke Cloud](#deployment-ke-cloud)
3. [Menjalankan Proyek](#menjalankan-proyek)
4. [Prasyarat](#prasyarat)
5. [Pengaturan Lingkungan](#pengaturan-lingkungan)
6. [Penggunaan](#penggunaan)

## Tujuan Proyek

Proyek ini bertujuan untuk mengumpulkan data podcast dari Spotify menggunakan API Spotify, membersihkan dan mentransformasikan data tersebut, kemudian mengunggahnya ke **Google BigQuery** untuk analisis lebih lanjut. Pipeline ini memungkinkan pengguna untuk mengotomatisasi proses pengumpulan dan pembersihan data podcast dengan mudah.

## Langkah-Langkah Proyek

### Extract: Mengambil Data Podcast
- **Autentikasi dengan Spotify API** menggunakan `client_id` dan `client_secret`.
- Mengambil metadata podcast dan daftar episode yang relevan.
- Menyimpan hasil ekstraksi dalam file CSV.

### Transform: Pembersihan dan Transformasi Data
- Membersihkan dan mentransformasi data yang telah diekstrak.
- Mengonversi durasi episode dari milidetik ke menit.
- Menghitung panjang deskripsi dan jumlah kata di dalam judul episode.
- Menyimpan data yang sudah diproses dalam file CSV yang bersih.

### Load: Mengunggah Data ke BigQuery
- Menggunakan `pandas_gbq` untuk mengunggah data yang telah dibersihkan ke **Google BigQuery**.
- Menyimpan data dalam tabel BigQuery yang dapat digunakan untuk analisis lebih lanjut.

### Menjalankan Pipeline
- Menggunakan **Prefect** untuk menjalankan pipeline secara otomatis dan mengelola alur kerja (flow).
- Proses ETL dijalankan dalam urutan Extract, Transform, dan Load.

### Deployment ke Cloud
- Meng-deploy pipeline ke **Prefect Cloud** menggunakan GitHub sebagai sumber kode.
- Pipeline dapat dijalankan secara manual atau otomatis di cloud menggunakan Prefect.

## Menjalankan Proyek

Untuk menjalankan proyek ini secara lokal atau di cloud, pastikan Anda telah menyiapkan lingkungan berikut:

### Prasyarat
- Python 3.x
- Prefect
- Pandas
- pandas_gbq
- GCP Credentials
- Spotify API Keys (client_id dan client_secret)

### Pengaturan Lingkungan

1. **Clone repository** ini ke mesin lokal Anda:
   ```bash
   git clone https://github.com/dennisbnrd/spotify-etl.git
   cd spotify-etl
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
3. Setup .env file:
   - Salin template .env.example ke .env.
   - Masukkan Spotify API credentials (CLIENT_ID, CLIENT_SECRET) dan GCP credentials.

### Penggunaan
1. Menjalankan ETL pipeline secara lokal:
  ```
  python main.py
  ```
2. Deployment ke Prefect Cloud:
   - Pipeline ini dapat dideploy ke Prefect Cloud menggunakan kode di dalam deploy.py.

3. Jadwalkan dan jalankan pipeline di Prefect Cloud sesuai kebutuhan Anda.
