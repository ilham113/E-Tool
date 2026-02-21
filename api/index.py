import os
import io
import pandas as pd
from flask import Flask, render_template, jsonify, send_file, request
from datetime import datetime

app = Flask(__name__, template_folder='../templates')

# Konfigurasi Path untuk Vercel (/tmp bersifat writeable)
DATA_DIR = '/tmp/data'
KIRIM_DIR = '/tmp/kirim'

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KIRIM_DIR, exist_ok=True)

def parse_all_logs():
    all_data = []
    if not os.path.exists(DATA_DIR):
        return pd.DataFrame()

    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.txt'):
            path = os.path.join(DATA_DIR, file_name)
            with open(path, 'r', encoding='utf-8') as f:
                for line in f.readlines():
                    hexdata = line.strip()
                    if len(hexdata) < 94: continue
                    
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
    df = parse_all_logs()
    if df.empty:
        return jsonify({'all': [], 'overview': []})
    
    all_transactions = df.to_dict(orient='records')
    overview = df.groupby('filename').agg(
        total_trx=('no_kartu', 'count'),
        total_amount=('tarif', 'sum')
    ).reset_index().to_dict(orient='records')
    
    return jsonify({'all': all_transactions, 'overview': overview})

@app.route('/api/settlement/process', methods=['POST'])
def process_settlement():
    """Mengimplementasikan logika settlement sesuai kode .ipynb Anda"""
    data_req = request.get_json()
    selected_filenames = data_req.get('filenames', [])
    
    df_raw = parse_all_logs()
    if df_raw.empty or not selected_filenames:
        return jsonify([])

    # Filter data berdasarkan file yang dipilih
    bayar_df = df_raw[df_raw['filename'].isin(selected_filenames)].copy()
    
    # Ambil MID/TID unik dari data yang difilter (seperti pintu_df)
    pintu_df = bayar_df[['mid', 'tid']].drop_duplicates()
    
    setel_df = pd.DataFrame(columns=['filename_source', 'tanggal', 'wktsetel', 'mid', 'tid', 'versi', 'bat', 'trxcount', 'trxamount'])
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")

    # Proses per pintu (MID/TID) sesuai logika Anda
    for _, b in pintu_df.iterrows():
        mid, tid = b['mid'], b['tid']
        semua = bayar_df[(bayar_df['tid'] == tid) & (bayar_df['mid'] == mid)]
        maxbat = (len(semua) // 999) + 1

        for x in range(maxbat):
            bat_num = x + 1
            isibat = f"{bat_num:03d}"
            
            data_batch = semua.iloc[x*999 : (x+1)*999]
            if not data_batch.empty:
                trxamount = data_batch['tarif'].sum()
                row_setel = {
                    'filename_source': ", ".join(selected_filenames),
                    'tanggal': datetime.now().date().isoformat(),
                    'wktsetel': wkt,
                    'mid': mid,
                    'tid': tid,
                    'versi': '01',
                    'bat': isibat,
                    'trxcount': len(data_batch),
                    'trxamount': int(trxamount)
                }
                setel_df = pd.concat([setel_df, pd.DataFrame([row_setel])], ignore_index=True)
                
                # Simpan file fisik di /tmp/kirim
                heder = f"{len(data_batch):03d}{int(trxamount):010d}"
                nama_file = f"{wkt}{mid}{tid}01{isibat}.txt"
                filepath = os.path.join(KIRIM_DIR, nama_file)
                
                with open(filepath, "w") as f:
                    f.write(heder + "\n")
                    for _, row_trx in data_batch.iterrows():
                        f.write(row_trx['respon'][14:] + "\n")

    return jsonify(setel_df.to_dict(orient='records'))

@app.route('/api/download_ready')
def download_ready():
    """Download semua file yang baru saja dibuat di /tmp/kirim"""
    files = [f for f in os.listdir(KIRIM_DIR)]
    return jsonify(files)

@app.route('/api/get_file/<name>')
def get_file(name):
    return send_file(os.path.join(KIRIM_DIR, name), as_attachment=True)

@app.route('/api/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    if file:
        file.save(os.path.join(DATA_DIR, file.filename))
        return jsonify({'message': 'Success'})
    return jsonify({'error': 'No file'}), 400

@app.route('/api/clear-data', methods=['POST'])
def clear_data():
    for folder in [DATA_DIR, KIRIM_DIR]:
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    app.run(debug=True)