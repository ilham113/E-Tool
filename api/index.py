import os
import io
import shutil
import pandas as pd
from flask import Flask, render_template, jsonify, send_file, request
from datetime import datetime

app = Flask(__name__, template_folder='../templates')

# Konfigurasi Path untuk Vercel (Penyimpanan Sementara di RAM/Disk Temp)
DATA_DIR = '/tmp/data'
KIRIM_DIR = '/tmp/kirim'

# Pastikan direktori tersedia saat runtime
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KIRIM_DIR, exist_ok=True)

def parse_all_logs():
    """Memproses semua file .txt di folder /tmp/data dengan parsing hex sesuai notebook"""
    all_data = []
    if not os.path.exists(DATA_DIR):
        return pd.DataFrame()

    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.txt'):
            path = os.path.join(DATA_DIR, file_name)
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    hexdata = line.strip()
                    if len(hexdata) < 94: continue
                    
                    # Menambahkan prefix 0200a900000000 sesuai logika pemrosesan
                    full_hex = '0200a900000000' + hexdata
                    try:
                        all_data.append({
                            'filename': file_name,
                            'mid': full_hex[16:32],
                            'tid': full_hex[32:40],
                            'kode_bank': full_hex[14:16],
                            'no_kartu': full_hex[54:70],
                            'tarif': int(full_hex[70:78], 16),
                            'saldo': int(full_hex[78:86], 16),
                            'trx_date': f"{full_hex[40:42]}-{full_hex[42:44]}-{full_hex[44:48]} {full_hex[48:50]}:{full_hex[50:52]}:{full_hex[52:54]}",
                            'respon': full_hex
                        })
                    except: continue
    return pd.DataFrame(all_data)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/init')
def get_initial_data():
    """Mengambil seluruh transaksi dan daftar file untuk Overview"""
    df = parse_all_logs()
    if df.empty:
        return jsonify({'all': [], 'overview': []})
    
    all_transactions = df.to_dict(orient='records')
    overview = df.groupby('filename').agg(
        total_trx=('no_kartu', 'count'),
        total_amount=('tarif', 'sum')
    ).reset_index().to_dict(orient='records')
    
    return jsonify({'all': all_transactions, 'overview': overview})

@app.route('/api/settlement/multiple', methods=['POST'])
def get_settlement_multiple():
    """Proses agregasi settlement untuk multiple files yang dipilih di UI"""
    data = request.get_json()
    filenames = data.get('filenames', [])
    
    df = parse_all_logs()
    if df.empty or not filenames:
        return jsonify([])

    # Filter data berdasarkan list file yang dipilih user
    filtered = df[df['filename'].isin(filenames)]
    
    settlement = filtered.groupby(['mid', 'tid']).agg(
        trxcount=('no_kartu', 'count'),
        trxamount=('tarif', 'sum')
    ).reset_index()
    
    settlement['wktsetel'] = datetime.now().strftime("%Y%m%d%H%M%S")
    return jsonify(settlement.to_dict(orient='records'))

@app.route('/api/download_multiple')
def download_multiple():
    """Generate file gabungan .txt untuk file yang dipilih dan simpan di /tmp/kirim"""
    files_param = request.args.get('files', '')
    if not files_param: return "No files selected", 400
    
    filenames = files_param.split(',')
    df = parse_all_logs()
    filtered = df[df['filename'].isin(filenames)]
    
    if filtered.empty: return "Data Not Found", 404

    # Format Header: Total Count (3 digit) + Total Amount (10 digit)
    header = f"{len(filtered):03d}{filtered['tarif'].sum():010d}"
    
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    nama_output = f"SETTLE_COMBINED_{wkt}.txt"
    file_path = os.path.join(KIRIM_DIR, nama_output)

    with open(file_path, 'w') as f:
        f.write(header + "\n")
        for _, row in filtered.iterrows():
            # Tulis data asli tanpa prefix 14 karakter pertama
            f.write(row['respon'][14:] + "\n")

    return send_file(file_path, as_attachment=True, download_name=nama_output)

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Endpoint untuk upload file .txt ke /tmp/data"""
    if 'file' not in request.files: return jsonify({'error': 'No file'}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({'error': 'No filename'}), 400
    
    file.save(os.path.join(DATA_DIR, file.filename))
    return jsonify({'message': 'Upload success'})

@app.route('/api/clear-data', methods=['POST'])
def clear_data():
    """Membersihkan folder /tmp/data dan /tmp/kirim secara fisik"""
    for folder in [DATA_DIR, KIRIM_DIR]:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                os.remove(os.path.join(folder, file))
    return jsonify({'status': 'success', 'message': 'Storage cleared.'})

if __name__ == '__main__':
    app.run(debug=True)