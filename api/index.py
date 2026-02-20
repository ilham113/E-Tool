import os
import pandas as pd
import shutil
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

# Cari path folder saat ini (folder /api)
base_dir = os.path.dirname(os.path.abspath(__file__))

# Tentukan lokasi folder templates yang berada di root (sejajar dengan folder api)
template_dir = os.path.join(base_dir, '..', 'templates')

app = Flask(__name__, template_folder=template_dir)
app.secret_key = 'duta1234' # Untuk flash message

# Konfigurasi Folder
UPLOAD_FOLDER = '/tmp/data'
OUTPUT_FOLDER = '/tmp/kirim'
LOG_FOLDER = '/tmp/logs'

for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER, LOG_FOLDER]:
    os.makedirs(folder, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# ---------------------------------------------------------
# KONFIGURASI LOGGING (DAILY)
# ---------------------------------------------------------
log_filename = os.path.join(LOG_FOLDER, "log_kartu.log")
handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=30)
handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger("KartuLogger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# ---------------------------------------------------------
# LOGIKA PEMROSESAN DATA
# ---------------------------------------------------------
def process_emoney_data():
    df = pd.DataFrame(columns=['mid', 'tid', 'kode_bank', 'no_kartu', 'saldo', 'tarif', 'counter', 'trx_date', 'waktu_unik', 'respon'])
    file_list = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('.txt')]

    if not file_list:
        return False

    for nama_file in file_list:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], nama_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

            for idx, line in enumerate(lines):
                hexdata = line.strip()
                if len(hexdata) < 94:
                    logger.warning(f"Data terlalu pendek di {nama_file} baris {idx+1}")
                    continue
                
                # Header dummy sesuai notebook
                prefix = '0200a900000000'
                full_hex = prefix + hexdata
                
                try:
                    new_row = {
                        'mid': full_hex[16:32],
                        'tid': full_hex[32:40],
                        'kode_bank': full_hex[14:16],
                        'no_kartu': full_hex[54:70],
                        'saldo': int(full_hex[78:86], 16),
                        'tarif': int(full_hex[70:78], 16),
                        'counter': int(full_hex[86:94], 16),
                        'trx_date': f"{full_hex[40:42]}-{full_hex[42:44]}-{full_hex[44:48]}",
                        'waktu_unik': full_hex[40:54],
                        'respon': full_hex,
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error parsing hex di {nama_file}: {e}")
        except Exception as e:
            logger.error(f"Gagal membaca file {nama_file}: {e}")

    if df.empty: return False

    # Logika Settlement (Pemisahan per TID/MID)
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    tids = df['tid'].unique()

    for tid in tids:
        subset = df[df['tid'] == tid]
        mid = subset.iloc[0]['mid']
        
        # Limit 999 baris per batch sesuai notebook
        data_batch = subset.head(999)
        trxcount = len(data_batch)
        trxamount = data_batch['tarif'].sum()
        
        bat = "001"
        nama_file_out = f"{wkt}{mid}{tid}01{bat}.txt"
        filepath = os.path.join(app.config['OUTPUT_FOLDER'], nama_file_out)

        with open(filepath, "w") as f:
            f.write(f"{trxcount:03d}{trxamount:010d}\n")
            for _, row in data_batch.iterrows():
                # Tulis data tanpa prefix 14 karakter pertama (kembali ke format asli)
                f.write(row['respon'][14:] + "\n")
        
        logger.info(f"SUCCESS | File Created: {nama_file_out} | Trx: {trxcount} | Amount: {trxamount}")

    return True

# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------
@app.route('/')
def index():
    files = sorted([f for f in os.listdir(app.config['OUTPUT_FOLDER']) if f.endswith('.txt')], reverse=True)
    return render_template('index.html', files=files)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files' not in request.files:
        flash('Tidak ada file terpilih', 'danger')
        return redirect(request.url)
    
    files = request.files.getlist('files')
    uploaded_count = 0
    for file in files:
        if file.filename != '':
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
            uploaded_count += 1
    
    if uploaded_count > 0:
        if process_emoney_data():
            flash(f'Berhasil memproses {uploaded_count} file.', 'success')
        else:
            flash('File berhasil diunggah tapi gagal diproses (Format tidak sesuai).', 'warning')
    
    return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename)

@app.route('/delete/<filename>')
def delete_file(filename):
    try:
        os.remove(os.path.join(app.config['OUTPUT_FOLDER'], filename))
        flash(f'File {filename} dihapus.', 'info')
    except:
        flash('Gagal menghapus file.', 'danger')
    return redirect(url_for('index'))

@app.route('/clear-all', methods=['POST'])
def clear_all():
    # Bersihkan input dan output
    for folder in [app.config['UPLOAD_FOLDER'], app.config['OUTPUT_FOLDER']]:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
    
    logger.info("STORAGE | Semua data dibersihkan oleh user.")
    flash('Semua file (Data & Settlement) telah dibersihkan!', 'success')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)