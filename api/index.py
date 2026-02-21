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
    """Memproses file .txt dengan parsing hexadesimal"""
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
                    
                    # Logic notebook: prefix + parsing
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

@app.route('/api/settlement/multiple', methods=['POST'])
def get_settlement_multiple():
    """Agregasi data settlement dengan menampilkan filename"""
    data = request.get_json()
    filenames = data.get('filenames', [])
    
    df = parse_all_logs()
    if df.empty or not filenames:
        return jsonify([])

    # Grouping menyertakan filename untuk tampilan tabel
    filtered = df[df['filename'].isin(filenames)]
    settlement = filtered.groupby(['filename', 'mid', 'tid']).agg(
        trxcount=('no_kartu', 'count'),
        trxamount=('tarif', 'sum')
    ).reset_index()
    
    settlement['wktsetel'] = datetime.now().strftime("%Y%m%d%H%M%S")
    return jsonify(settlement.to_dict(orient='records'))

@app.route('/api/download_single/<filename>')
def download_single(filename):
    """Download individual per file sesuai format settlement"""
    df = parse_all_logs()
    filtered = df[df['filename'] == filename]
    
    if filtered.empty: return "Not Found", 404

    # Header: Count(3) + Amount(10)
    header = f"{len(filtered):03d}{filtered['tarif'].sum():010d}"
    
    wkt = datetime.now().strftime("%Y%m%d%H%M%S")
    mid, tid = filtered.iloc[0]['mid'], filtered.iloc[0]['tid']
    nama_output = f"{wkt}{mid}{tid}01001.txt"
    
    output = io.StringIO()
    output.write(header + "\n")
    for _, row in filtered.iterrows():
        output.write(row['respon'][14:] + "\n") # Potong prefix

    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8'))
    mem.seek(0)
    
    return send_file(mem, as_attachment=True, download_name=nama_output, mimetype='text/plain')

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