from flask import Flask, request, jsonify
import yaml
import subprocess
import os

app = Flask(__name__)

# Directory to store replication configs
CONFIG_DIR = './configs'
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

# Endpoint to set a new connection
@app.route('/set_connection', methods=['POST'])
def set_connection():
    data = request.json
    conn_name = data['name']
    conn_details = data['details']

    try:
        command = ['sling', 'conns', 'set', conn_name]
        for key, value in conn_details.items():
            if key == 'url':
                # Handle the URL separately to avoid issues with special characters
                command.append(f"url='{value}'")
            else:
                command.append(f"{key}={value}")
        
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({'status': 'error', 'message': result.stderr}), 500
        return jsonify({'status': 'success', 'message': 'Connection set successfully'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Endpoint to register a new connection and create a replication config
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['source']['stream']}.yaml")
    
    # Build replication config
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
    
    # Write config to a YAML file
    with open(config_file, 'w') as file:
        yaml.dump(config, file)
    
    return jsonify({'status': 'success', 'config_file': config_file}), 201

# Endpoint to run a data sync job
@app.route('/run', methods=['POST'])
def run():
    data = request.json
    config_file = os.path.join(CONFIG_DIR, f"{data['stream']}.yaml")
    
    if not os.path.exists(config_file):
        return jsonify({'status': 'error', 'message': 'Config file not found'}), 404
    
    try:
        # Run the replication using Sling CLI
        result = subprocess.run(['sling', 'run', '-r', config_file], capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({'status': 'error', 'message': result.stderr}), 500
        return jsonify({'status': 'success', 'output': result.stdout}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Endpoint to get the status of a job (mock example)
@app.route('/status', methods=['GET'])
def status():
    # Implement job status retrieval logic here
    # This can be integrated with a job management system if needed
    return jsonify({'status': 'running', 'details': 'Job is currently running'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)