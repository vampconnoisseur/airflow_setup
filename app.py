from flask import Flask, request, jsonify
import yaml
import subprocess
import os
from urllib.parse import quote
import json

app = Flask(__name__)

CONFIG_DIR = './configs'
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

def run_sling_command(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return {'status': 'error', 'message': result.stderr}, 500
        return {'status': 'success', 'output': result.stdout}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/set_connection', methods=['POST'])
def set_connection():
    data = request.json
    conn_name = data['conn']
    conn_details = data['details']

    command = ['sling', 'conns', 'set', conn_name]
    for key, value in conn_details.items():
        if key == 'url':
            # URL encode the value and wrap it in double quotes
            encoded_url = quote(value, safe='')
            command.append(f'url="{encoded_url}"')
        else:
            command.append(f"{key}={value}")
    
    result, status_code = run_sling_command(command)
    return jsonify(result), status_code

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['source']['stream']}.yaml")
    
    config = {
        'source': data['source']['conn'],
        'target': data['target']['conn'],
        'defaults': {
            'mode': data.get('mode', 'full-refresh'),
            'object': data['target']['object']
        },
        'streams': {
            data['source']['stream']: {}
        }
    }
    
    if 'source_options' in data:
        config['defaults']['source_options'] = data['source_options']
    if 'target_options' in data:
        config['defaults']['target_options'] = data['target_options']
    
    with open(config_file, 'w') as file:
        yaml.dump(config, file)
    
    return jsonify({'status': 'success', 'config_file': config_file}), 201

@app.route('/run', methods=['POST'])
def run():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['stream']}.yaml")
    
    if not os.path.exists(config_file):
        return jsonify({'status': 'error', 'message': 'Config file not found'}), 404
    
    command = ['sling', 'run', '-r', config_file]
    result, status_code = run_sling_command(command)
    return jsonify(result), status_code

@app.route('/run_custom', methods=['POST'])
def run_custom():
    data = request.json
    command = ['sling', 'run']
    
    for key in ['src_conn', 'src_stream', 'tgt_conn', 'tgt_object', 'mode']:
        if key in data:
            command.extend([f'--{key.replace("_", "-")}', data[key]])
    
    for key in ['src_options', 'tgt_options']:
        if key in data:
            command.extend([f'--{key.replace("_", "-")}', json.dumps(data[key])])
    
    result, status_code = run_sling_command(command)
    return jsonify(result), status_code

@app.route('/status', methods=['GET'])
def status():
    # Implement job status retrieval logic here
    # This is a placeholder implementation
    return jsonify({'status': 'running', 'details': 'Job is currently running'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)