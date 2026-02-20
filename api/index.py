import os
import pandas as pd
import shutil
import json
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from datetime import datetime

# 1. SETUP PATH
base_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(base_dir, '..', 'templates')

app = Flask(__name__, template_folder=template_dir)
app.secret_key = 'duta1234'

# 2. KONFIGURASI FOLDER
UPLOAD_FOLDER = '/tmp/data'
OUTPUT_FOLDER = '/tmp/kirim'
CONFIG_FILE = '/tmp/error_codes.txt'
ALL_DATA_FILE = '/tmp/all_transaksi.json'

def ensure_dirs():
    for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    if not os.path.exists(CONFIG_FILE):
        original_path = os.path.join(base_dir, 'error_codes.txt')
        if os.path.exists(original_path):
            shutil.copy(original_path, CONFIG_FILE)
        else:
            with open(CONFIG_FILE, 'w') as f:
                f.write("03=Count Mismatch\n")

def clean_error_codes():
    """Menghapus kode 02 dari file config jika ada untuk proteksi sistem."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            lines = f.readlines()
        with open(CONFIG_FILE, 'w') as f:
            for line in lines:
                if not line.startswith('02='):
                    f.write(line)

def get_error_mapping():
    mapping = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            for line in f:
                if '=' in line:
                    code, desc = line.strip().split('=', 1)
                    mapping[code] = desc
    mapping['02'] = "Duplicate Data (Auto-Skip)"
    return mapping

# 3. ROUTES UTAMA
@app.route('/upload', methods=['POST'])
def upload_file():
    ensure_dirs()
    if request.form.get('password') != 'Duta@321':
        flash('Password Auth Salah!', 'danger')
        return redirect(url_for('index'))
        
    data_type = request.form.get('data_type', 'raw')
    files = request.files.getlist('files')
    
    all_data = []
    if os.path.exists(ALL_DATA_FILE):
        with open(ALL_DATA_FILE, 'r') as f:
            try: all_data = json.load(f)
            except: all_data = []

    for file in files:
        if file.filename:
            file_path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(file_path)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    
                    # Logika deteksi error code di akhir baris untuk NOK
                    parts = line.split(" ")
                    err = parts[-1].replace('"', '') if data_type == 'nok' and len(parts) > 1 else ""
                    hexdata = parts[0] if data_type == 'nok' else line
                    
                    if len(hexdata) >= 94:
                        full_hex = '0200a900000000' + hexdata
                        try:
                            all_data.append({
                                'mid': full_hex[16:32],
                                'tid': full_hex[32:40],
                                'tarif': int(full_hex[70:78], 16),
                                'respon': full_hex,
                                'error_code': err,
                                'filename': file.filename,
                                'type': data_type
                            })
                        except: continue
    
    with open(ALL_DATA_FILE, 'w') as f: json.dump(all_data, f)
    flash(f'{len(files)} File berhasil dibaca. Silahkan pilih file untuk settlement.', 'info')
    return redirect(url_for('index'))

@app.route('/process_settlement', methods=['POST'])
def process_settlement():
    selected_files = request.form.getlist('selected_files')
    if not selected_files:
        flash('Pilih minimal satu file dari overview!', 'warning')
        return redirect(url_for('index'))

    if not os.path.exists(ALL_DATA_FILE): return redirect(url_for('index'))
    
    with open(ALL_DATA_FILE, 'r') as f: all_data = json.load(f)
    df = pd.DataFrame(all_data)

    # FILTER: Berdasarkan filename yang dipilih DAN SKIP ERROR 02
    df_filtered = df[(df['filename'].isin(selected_files)) & (df['error_code'] != '02')]

    if df_filtered.empty:
        flash('Tidak ada data yang perlu di-settlement (Semua data ter-filter 02 atau kosong).', 'danger')
        return redirect(url_for('index'))

    # Grouping per TID untuk Output
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    for tid in df_filtered['tid'].unique():
        subset = df_filtered[df_filtered['tid'] == tid]
        mid = subset.iloc[0]['mid']
        nama_out = f"{wkt}{mid}{tid}01001.txt"
        with open(os.path.join(OUTPUT_FOLDER, nama_out), "w") as f:
            f.write(f"{len(subset):03d}{subset['tarif'].sum():010d}RAW\n")
            for _, row in subset.iterrows():
                f.write(row['respon'][14:] + "\n")
    
    flash('Settlement Berhasil (Data 02 otomatis dilewati).', 'success')
    return redirect(url_for('index'))

@app.route('/')
def index():
    ensure_dirs()
    clean_error_codes()
    error_map = get_error_mapping()
    
    overview = []
    if os.path.exists(ALL_DATA_FILE):
        with open(ALL_DATA_FILE, 'r') as f:
            try:
                data = json.load(f)
                if data:
                    df = pd.DataFrame(data)
                    for fname in df['filename'].unique():
                        f_df = df[df['filename'] == fname]
                        err_counts = f_df[f_df['error_code'] != ""]['error_code'].value_counts().to_dict()
                        overview.append({
                            'filename': fname,
                            'total': len(f_df),
                            'nominal': f_df['tarif'].sum(),
                            'errors': err_counts
                        })
            except: pass

    settlements = []
    if os.path.exists(OUTPUT_FOLDER):
        for f in sorted(os.listdir(OUTPUT_FOLDER), reverse=True):
            if f.endswith('.txt'):
                try:
                    with open(os.path.join(OUTPUT_FOLDER, f), 'r') as file:
                        header = file.readline().strip()
                        settlements.append({'name': f, 'count': int(header[:3]), 'amount': int(header[3:13])})
                except: continue

    return render_template('index.html', overview=overview, settlements=settlements, error_map=error_map)

@app.route('/config', methods=['GET', 'POST'])
def config_errors():
    ensure_dirs()
    clean_error_codes()
    auth = request.args.get('auth_pass') or request.form.get('auth_pass')
    if auth != 'setdutaparkir@321':
        return render_template('login_config.html')

    error_map = get_error_mapping()
    if request.method == 'POST':
        code = request.form.get('code')
        desc = request.form.get('desc')
        if 'add_code' in request.form:
            if code == "02":
                flash("Kode 02 diproteksi sistem!", "danger")
            elif code and desc:
                error_map[code] = desc
                with open(CONFIG_FILE, 'w') as f:
                    for k, v in error_map.items():
                        if k != '02': f.write(f"{k}={v}\n")
                flash(f'Kode {code} Disimpan', 'success')
        elif 'delete_code' in request.form:
            del_code = request.form.get('delete_code')
            if del_code in error_map and del_code != "02":
                del error_map[del_code]
                with open(CONFIG_FILE, 'w') as f:
                    for k, v in error_map.items():
                        if k != '02': f.write(f"{k}={v}\n")
                flash(f'Kode {del_code} Dihapus', 'danger')
    
    display_map = {k: v for k, v in error_map.items() if k != "02"}
    return render_template('config.html', error_map=display_map, auth_pass=auth)

@app.route('/clear-all', methods=['POST'])
def clear_all():
    for f in [ALL_DATA_FILE, UPLOAD_FOLDER, OUTPUT_FOLDER]:
        if os.path.isfile(f): os.remove(f)
        elif os.path.isdir(f):
            for file in os.listdir(f): os.remove(os.path.join(f, file))
    return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(OUTPUT_FOLDER, filename)