from flask import Flask, request, jsonify
import yaml
import subprocess
import os
import json
import logging

app = Flask(__name__)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

CONFIG_DIR = './configs'
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

def run_sling_command(command):
    try:
        app.logger.info(f"Executing command: {command}")
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return {'status': 'error', 'message': result.stderr}, 500
        return {'status': 'success', 'output': result.stdout}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/check', methods=['GET'])
def check():
    return "Hello, Sling API is up and running"

@app.route('/set_connection', methods=['POST'])
def set_connection():
    data = request.json
    conn_name = data['conn']
    conn_details = data['details']

    command = ['sling', 'conns', 'set', conn_name]
    
    for key, value in conn_details.items():
        command.append(f"{key}={value}")

    app.logger.info(f"Setting connection with command: {command}")
    result, status_code = run_sling_command(command)
    return jsonify(result), status_code

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['dag_id']}_replication.yaml")

    config = {
        'source': data['source']['conn'] if data['source'].get('conn') else 'local://',
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
        config['streams'][data['source']['stream']]['source_options'] = data['source_options']
    if 'target_options' in data:
        config['streams'][data['source']['stream']]['target_options'] = data['target_options']
    if 'primary_key' in data:
        config['defaults']['primary_key'] = data['primary_key']
    if 'update_key' in data:
        config['defaults']['update_key'] = data['update_key']

    app.logger.info(f"Creating replication config file: {config_file}")
    with open(config_file, 'w') as file:
        yaml.dump(config, file)

    return jsonify({'status': 'success', 'config_file': config_file}), 201

@app.route('/run', methods=['POST'])
def run():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['dag_id']}_replication.yaml")

    if not os.path.exists(config_file):
        return jsonify({'status': 'error', 'message': 'Config file not found'}), 404

    command = ['sling', 'run', '-r', config_file]
    result, status_code = run_sling_command(command)
    return jsonify(result), status_code

@app.route('/status', methods=['GET'])
def status():
    # Placeholder implementation for job status retrieval
    return jsonify({'status': 'running', 'details': 'Job is currently running'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
