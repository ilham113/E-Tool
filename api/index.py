import os
import io
import shutil
import pandas as pd
from flask import Flask, render_template, jsonify, send_file, request
from datetime import datetime

app = Flask(__name__, template_folder='../templates')

# Konfigurasi Path untuk Vercel (Penyimpanan Sementara)
DATA_DIR = '/tmp/data'
KIRIM_DIR = '/tmp/kirim'

# Pastikan folder tersedia di lingkungan runtime
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KIRIM_DIR, exist_ok=True)

def parse_all_logs():
    """Fungsi untuk memproses semua file .txt di folder /tmp/data"""
    all_data = []
    if not os.path.exists(DATA_DIR):
        return pd.DataFrame()

    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.txt'):
            path = os.path.join(DATA_DIR, file_name)
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for idx, line in enumerate(lines):
                    hexdata = line.strip()
                    if len(hexdata) < 94:
                        continue
                    
                    # Logic: Tambahkan prefix sesuai tool_emoney_.ipynb
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
                    except:
                        continue
    return pd.DataFrame(all_data)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/init')
def get_initial_data():
    """Mengambil data All Transactions dan Overview Filename"""
    df = parse_all_logs()
    if df.empty:
        return jsonify({'all': [], 'overview': []})
    
    all_transactions = df.to_dict(orient='records')
    overview = df.groupby('filename').agg(
        total_trx=('no_kartu', 'count'),
        total_amount=('tarif', 'sum')
    ).reset_index().to_dict(orient='records')
    
    return jsonify({'all': all_transactions, 'overview': overview})

@app.route('/api/settlement/<filename>')
def get_settlement(filename):
    """Mengambil data agregasi (Settlement) untuk file tertentu"""
    df = parse_all_logs()
    filtered = df[df['filename'] == filename]
    
    if filtered.empty:
        return jsonify([])

    settlement = filtered.groupby(['mid', 'tid']).agg(
        trxcount=('no_kartu', 'count'),
        trxamount=('tarif', 'sum')
    ).reset_index()
    
    settlement['wktsetel'] = datetime.now().strftime("%Y%m%d%H%M%S")
    return jsonify(settlement.to_dict(orient='records'))

@app.route('/api/download/<filename>')
def download_settlement(filename):
    """Menghasilkan file .txt settlement di /tmp/kirim dan mengirimnya ke user"""
    df = parse_all_logs()
    filtered = df[df['filename'] == filename]
    
    if filtered.empty:
        return "Data Not Found", 404

    # Format Header: Count(3 digit) + Amount(10 digit)
    header = f"{len(filtered):03d}{filtered['tarif'].sum():010d}"
    
    # Generate Nama File Settlement
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    mid = filtered.iloc[0]['mid']
    tid = filtered.iloc[0]['tid']
    nama_file_setel = f"{wkt}{mid}{tid}01001.txt"
    
    file_path = os.path.join(KIRIM_DIR, nama_file_setel)

    with open(file_path, 'w') as f:
        f.write(header + "\n")
        for _, row in filtered.iterrows():
            # Mengambil data asli tanpa prefix (0200a900000000)
            f.write(row['respon'][14:] + "\n")

    return send_file(file_path, as_attachment=True, download_name=nama_file_setel)

@app.route('/api/clear-data', methods=['POST'])
def clear_data():
    """Menghapus semua file di folder /tmp/data dan /tmp/kirim"""
    deleted_count = 0
    for folder in [DATA_DIR, KIRIM_DIR]:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                os.remove(os.path.join(folder, file))
                deleted_count += 1
    return jsonify({'status': 'success', 'message': f'Berhasil menghapus {deleted_count} file.'})

# Handler untuk Upload (Opsional agar bisa testing di Vercel)
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    file.save(os.path.join(DATA_DIR, file.filename))
    return jsonify({'message': 'File uploaded successfully'})

if __name__ == '__main__':
    app.run(debug=True)