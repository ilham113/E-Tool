import os
import io
import math
import pandas as pd
from flask import Flask, render_template, jsonify, send_file, request
from datetime import datetime

app = Flask(__name__, template_folder='../templates')

# Konfigurasi Path untuk Vercel
DATA_DIR = '/tmp/data'
KIRIM_DIR = '/tmp/kirim'

# Maksimum transaksi per file (batch size)
MAX_TRX_PER_FILE = 999

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KIRIM_DIR, exist_ok=True)

def parse_all_logs():
    all_data = []
    if not os.path.exists(DATA_DIR):
        return pd.DataFrame()

    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.txt'):
            path = os.path.join(DATA_DIR, file_name)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f.readlines():
                        hexdata = line.strip()
                        if len(hexdata) < 94: continue
                        
                        full_hex = '0200a900000000' + hexdata
                        all_data.append({
                            'filename': file_name,
                            'mid': full_hex[16:32],
                            'tid': full_hex[32:40],
                            'tarif': int(full_hex[70:78], 16),
                            'no_kartu': full_hex[54:70],
                            'trx_date': f"{full_hex[40:42]}-{full_hex[42:44]}-{full_hex[44:48]} {full_hex[48:50]}:{full_hex[50:52]}:{full_hex[52:54]}",
                            'respon': full_hex
                        })
            except Exception: continue
    return pd.DataFrame(all_data)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/init')
def get_initial_data():
    df = parse_all_logs()
    if df.empty:
        return jsonify({'all': [], 'overview': []})
    
    all_transactions = df.to_dict(orient='records')
    # Perbaikan: Pastikan kolom filename ada setelah groupby
    overview = df.groupby('filename').agg(
        total_trx=('no_kartu', 'count'),
        total_amount=('tarif', 'sum')
    ).reset_index().to_dict(orient='records')
    
    return jsonify({'all': all_transactions, 'overview': overview})

@app.route('/api/settlement/process', methods=['POST'])
def process_settlement():
    data_req = request.get_json()
    selected_filenames = data_req.get('filenames', [])
    df_raw = parse_all_logs()
    
    if df_raw.empty or not selected_filenames:
        return jsonify([])

    bayar_df = df_raw[df_raw['filename'].isin(selected_filenames)].copy()
    pintu_df = bayar_df[['mid', 'tid']].drop_duplicates()
    
    results = []
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")

    for _, b in pintu_df.iterrows():
        mid, tid = b['mid'], b['tid']
        semua = bayar_df[(bayar_df['tid'] == tid) & (bayar_df['mid'] == mid)]
        maxbat = math.ceil(len(semua) / MAX_TRX_PER_FILE)

        for x in range(maxbat):
            bat_num = f"{(x + 1):03d}"
            start = x * MAX_TRX_PER_FILE
            end = (x + 1) * MAX_TRX_PER_FILE
            data_batch = semua.iloc[start:end]
            trxamount = data_batch['tarif'].sum()
            
            # Bangun nama file tanpa ekstensi; format: <wkt><mid><tid>01<bat(3)> (harus 43 chars)
            base_name = f"{wkt}{mid}{tid}01{bat_num}"

            # Pastikan panjang nama file tanpa ekstensi tepat 43 karakter.
            # Jika kurang => pad dengan '0' di kanan; jika lebih => potong ke 43.
            if len(base_name) < 43:
                base_name = base_name.ljust(43, '0')
            elif len(base_name) > 43:
                base_name = base_name[:43]

            nama_file = base_name + ".txt"
            filepath = os.path.join(KIRIM_DIR, nama_file)
            
            with open(filepath, "w") as f:
                f.write(f"{len(data_batch):03d}{int(trxamount):010d}\n")
                for _, row in data_batch.iterrows():
                    f.write(row['respon'][14:] + "\n")

            results.append({
                'mid': mid, 'tid': tid, 'bat': bat_num,
                'trxcount': len(data_batch), 'trxamount': int(trxamount),
                'download_path': nama_file
            })
    return jsonify(results)

@app.route('/api/get_file/<name>')
def get_file(name):
    return send_file(os.path.join(KIRIM_DIR, name), as_attachment=True)

@app.route('/api/upload', methods=['POST'])
def upload_file():
    files = request.files.getlist('file')
    for file in files:
        if file: file.save(os.path.join(DATA_DIR, file.filename))
    return jsonify({'message': 'Success'})

@app.route('/api/clear-data', methods=['POST'])
def clear_data():
    for folder in [DATA_DIR, KIRIM_DIR]:
        for f in os.listdir(folder): os.remove(os.path.join(folder, f))
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    app.run(debug=True)