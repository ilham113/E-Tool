import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

# 1. SETUP PATH TEMPLATES
base_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(base_dir, '..', 'templates')

app = Flask(__name__, template_folder=template_dir)
app.secret_key = 'duta1234'

# 2. KONFIGURASI FOLDER (Vercel mewajibkan penggunaan /tmp)
UPLOAD_FOLDER = '/tmp/data'
OUTPUT_FOLDER = '/tmp/kirim'
LOG_FOLDER = '/tmp/logs'

def ensure_dirs():
    """Memastikan direktori tersedia saat aplikasi dijalankan."""
    for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER, LOG_FOLDER]:
        os.makedirs(folder, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# 3. SETUP LOGGING
os.makedirs(LOG_FOLDER, exist_ok=True)
log_filename = os.path.join(LOG_FOLDER, "log_kartu.log")

log_handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=30)
log_handler.suffix = "%Y-%m-%d"
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger = logging.getLogger("KartuLogger")
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# ---------------------------------------------------------
# LOGIKA PEMROSESAN DATA
# ---------------------------------------------------------
def process_emoney_data(data_type='raw'):
    ensure_dirs()
    df = pd.DataFrame(columns=['mid', 'tid', 'kode_bank', 'no_kartu', 'saldo', 'tarif', 'counter', 'trx_date', 'waktu_unik', 'respon'])
    
    file_list = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith(('.txt', '.NOK'))]
    if not file_list:
        return False

    for nama_file in file_list:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], nama_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

            for idx, line in enumerate(lines):
                line = line.strip()
                if not line: continue
                
                if data_type == 'nok':
                    # Logika khusus file .NOK: split spasi & bersihkan suffix '03'
                    parts = line.split(" ")
                    hexdata = parts[0]
                    if hexdata.endswith("03"): 
                        hexdata = hexdata[:-2]
                else:
                    hexdata = line

                if len(hexdata) < 94:
                    continue
                
                full_hex = '0200a900000000' + hexdata
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
                    logger.error(f"Error parsing hex: {e}")
        except Exception as e:
            logger.error(f"Error reading file: {e}")

    if df.empty: return False

    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    for tid in df['tid'].unique():
        subset = df[df['tid'] == tid].head(999)
        mid = subset.iloc[0]['mid']
        nama_out = f"{wkt}{mid}{tid}01001.txt"
        filepath = os.path.join(app.config['OUTPUT_FOLDER'], nama_out)

        with open(filepath, "w") as f:
            # Header: Count (3 digit) + Amount (10 digit)
            f.write(f"{len(subset):03d}{subset['tarif'].sum():010d}\n")
            for _, row in subset.iterrows():
                f.write(row['respon'][14:] + "\n")
        
        logger.info(f"SUCCESS | Created: {nama_out}")
    return True

# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------
@app.route('/')
def index():
    ensure_dirs()
    files_data = []
    if os.path.exists(app.config['OUTPUT_FOLDER']):
        file_list = sorted([f for f in os.listdir(app.config['OUTPUT_FOLDER']) if f.endswith('.txt')], reverse=True)
        for filename in file_list:
            filepath = os.path.join(app.config['OUTPUT_FOLDER'], filename)
            try:
                with open(filepath, 'r') as f:
                    header = f.readline().strip()
                    # Ambil Count dan Amount dari header
                    count = int(header[:3])
                    amount = int(header[3:])
                files_data.append({'name': filename, 'count': count, 'amount': amount})
            except:
                continue
    return render_template('index.html', files=files_data)

@app.route('/upload', methods=['POST'])
def upload_file():
    ensure_dirs()
    data_type = request.form.get('data_type', 'raw')
    files = request.files.getlist('files')
    for file in files:
        if file.filename:
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
    
    if process_emoney_data(data_type):
        flash(f'Berhasil memproses sebagai {data_type.upper()}', 'success')
    else:
        flash('Gagal memproses file.', 'warning')
    return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename)

@app.route('/clear-all', methods=['POST'])
def clear_all():
    ensure_dirs()
    for folder in [app.config['UPLOAD_FOLDER'], app.config['OUTPUT_FOLDER']]:
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))
    flash('Storage dibersihkan!', 'success')
    return redirect(url_for('index'))

app.debug = False