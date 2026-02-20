import os
import pandas as pd
import shutil
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from datetime import datetime

# 1. SETUP PATH
base_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(base_dir, '..', 'templates')

app = Flask(__name__, template_folder=template_dir)
app.secret_key = 'duta1234'

# 2. KONFIGURASI FOLDER (Vercel /tmp agar bisa Write)
UPLOAD_FOLDER = '/tmp/data'
OUTPUT_FOLDER = '/tmp/kirim'
CONFIG_FILE = '/tmp/error_codes.txt'

def ensure_dirs():
    """Memastikan folder dan file konfigurasi tersedia di /tmp."""
    for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    
    if not os.path.exists(CONFIG_FILE):
        original_path = os.path.join(base_dir, 'error_codes.txt')
        if os.path.exists(original_path):
            shutil.copy(original_path, CONFIG_FILE)
        else:
            with open(CONFIG_FILE, 'w') as f:
                f.write("03=Count Mismatch\n02=Duplicate Data\n")

def get_error_mapping():
    mapping = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            for line in f:
                if '=' in line:
                    code, desc = line.strip().split('=', 1)
                    mapping[code] = desc
    return mapping

def save_error_mapping(mapping):
    with open(CONFIG_FILE, 'w') as f:
        for code, desc in mapping.items():
            f.write(f"{code}={desc}\n")

# 3. LOGIKA PEMROSESAN DATA
def process_emoney_data(data_type='raw'):
    ensure_dirs()
    error_map = get_error_mapping()
    registered_codes = sorted(list(error_map.keys()), key=len, reverse=True)
    
    file_list = [f for f in os.listdir(UPLOAD_FOLDER) if f.endswith(('.txt', '.NOK'))]
    if not file_list: return False

    for nama_file in file_list:
        df = pd.DataFrame(columns=['mid', 'tid', 'kode_bank', 'no_kartu', 'saldo', 'tarif', 'counter', 'trx_date', 'waktu_unik', 'respon', 'error_code'])
        file_details = {code: 0 for code in error_map.keys()}
        
        file_path = os.path.join(UPLOAD_FOLDER, nama_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            
            for line in lines:
                line = line.strip()
                if not line: continue
                
                error_code = ""
                if data_type == 'nok':
                    parts = line.split(" ")
                    hexdata = parts[0]
                    error_code = parts[-1] if len(parts) > 1 else ""
                    
                    if error_code in file_details:
                        file_details[error_code] += 1
                    
                    # SKIP KODE 02
                    if error_code == "02": continue
                    
                    # POTONG HEX DINAMIS
                    for code in registered_codes:
                        if hexdata.endswith(code):
                            hexdata = hexdata[:-len(code)]
                            break
                else:
                    hexdata = line

                if len(hexdata) < 94: continue
                
                full_hex = '0200a900000000' + hexdata
                try:
                    new_row = {
                        'mid': full_hex[16:32], 'tid': full_hex[32:40], 'kode_bank': full_hex[14:16],
                        'no_kartu': full_hex[54:70], 'saldo': int(full_hex[78:86], 16),
                        'tarif': int(full_hex[70:78], 16), 'counter': int(full_hex[86:94], 16),
                        'trx_date': f"{full_hex[40:42]}-{full_hex[42:44]}-{full_hex[44:48]}",
                        'waktu_unik': full_hex[40:54], 'respon': full_hex, 'error_code': error_code
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                except: continue

            if not df.empty:
                wkt = datetime.now().strftime("%Y%m%d%H%M%S")
                detail_str = "|".join([f"{c}:{v}" for c, v in file_details.items() if v > 0])
                
                for tid in df['tid'].unique():
                    subset = df[df['tid'] == tid].head(999)
                    mid = subset.iloc[0]['mid']
                    nama_out = f"{wkt}_{nama_file}_{tid}.txt"
                    
                    with open(os.path.join(OUTPUT_FOLDER, nama_out), "w") as f:
                        type_label = "RAW" if data_type == 'raw' else "NOK"
                        f.write(f"{len(subset):03d}{subset['tarif'].sum():010d}{type_label}{detail_str}\n")
                        for _, row in subset.iterrows():
                            f.write(row['respon'][14:] + "\n")
        except: continue
    return True

# 4. ROUTES
@app.route('/')
def index():
    ensure_dirs()
    error_map = get_error_mapping()
    files_data = []
    if os.path.exists(OUTPUT_FOLDER):
        file_list = sorted([f for f in os.listdir(OUTPUT_FOLDER) if f.endswith('.txt')], reverse=True)
        for filename in file_list:
            try:
                with open(os.path.join(OUTPUT_FOLDER, filename), 'r') as f:
                    header = f.readline().strip()
                    raw_details = header[16:].split('|') if len(header) > 16 else []
                    formatted_details = []
                    for d in raw_details:
                        if ':' in d:
                            c, v = d.split(':')
                            formatted_details.append(f"{v} {error_map.get(c, 'Error')}")
                    
                    files_data.append({
                        'name': filename,
                        'count': int(header[:3]),
                        'amount': int(header[3:13]),
                        'type': header[13:16],
                        'details': ", ".join(formatted_details)
                    })
            except: continue
    return render_template('index.html', files=files_data)

@app.route('/upload', methods=['POST'])
def upload_file():
    ensure_dirs()
    if request.form.get('password') != 'Duta@321':
        flash('Password Auth Salah!', 'danger')
        return redirect(url_for('index'))
    
    data_type = request.form.get('data_type', 'raw')
    files = request.files.getlist('files')
    for file in files:
        if file.filename: file.save(os.path.join(UPLOAD_FOLDER, file.filename))
    
    process_emoney_data(data_type)
    return redirect(url_for('index'))

@app.route('/config', methods=['GET', 'POST'])
def config_errors():
    ensure_dirs()
    auth = request.args.get('auth_pass') or request.form.get('auth_pass')
    if auth != 'setdutaparkir@321':
        return render_template('login_config.html')

    error_map = get_error_mapping()
    if request.method == 'POST':
        if 'add_code' in request.form:
            code, desc = request.form.get('code'), request.form.get('desc')
            if code and desc:
                error_map[code] = desc
                save_error_mapping(error_map)
                flash(f'Kode {code} Berhasil Diperbarui', 'success')
        elif 'delete_code' in request.form:
            code = request.form.get('delete_code')
            if code in error_map:
                del error_map[code]
                save_error_mapping(error_map)
                flash(f'Kode {code} Dihapus', 'danger')
    return render_template('config.html', error_map=error_map, auth_pass=auth)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(OUTPUT_FOLDER, filename)

@app.route('/clear-all', methods=['POST'])
def clear_all():
    ensure_dirs()
    for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER]:
        for f in os.listdir(folder): os.remove(os.path.join(folder, f))
    flash('Storage Bersih!', 'success')
    return redirect(url_for('index'))