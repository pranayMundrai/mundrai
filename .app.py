from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import csv
from io import StringIO



app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://pranay:1998@localhost/data_management'
db = SQLAlchemy(app)

class Dataset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    data = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"Dataset('{self.name}')"

@app.route('/dataset', methods=['POST'])
def upload_dataset():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file:
        try:
            stream = StringIO(file.stream.read().decode("UTF8"), newline=None)
            csv_data = csv.reader(stream)
            headers = next(csv_data)  
            data = [row for row in csv_data]
        except Exception as e:
            return jsonify({'error': str(e)}), 400

       
        dataset_name = file.filename.split('.')[0]  
        dataset = Dataset(name=dataset_name, data=str(data))
        db.session.add(dataset)
        db.session.commit()

        return jsonify({'message': 'Dataset uploaded successfully'}), 200

@app.route('/dataset', methods=['GET'])
def get_datasets():
    datasets = Dataset.query.all()
    dataset_list = [{'id': dataset.id, 'name': dataset.name} for dataset in datasets]
    return jsonify(dataset_list), 200

@app.route('/dataset/<int:id>/compute', methods=['POST'])
def compute_operation(id):
    dataset = Dataset.query.get(id)
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404

    req_data = request.json
    column_name = req_data.get('column_name')
    operation = req_data.get('operation')

    if not column_name or operation not in ['min', 'max', 'sum']:
        return jsonify({'error': 'Invalid request'}), 400

    data = eval(dataset.data)  

    try:
        column_index = dataset.data[0].index(column_name)
    except ValueError:
        return jsonify({'error': 'Column not found'}), 400

    column_values = [float(row[column_index]) for row in data]
    result = None

    if operation == 'min':
        result = min(column_values)
    elif operation == 'max':
        result = max(column_values)
    elif operation == 'sum':
        result = sum(column_values)

    return jsonify({'result': result}), 200

@app.route('/dataset/<int:id>/plot', methods=['GET'])
def get_plot_data(id):
    dataset = Dataset.query.get(id)
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404

    req_data = request.args
    column1 = req_data.get('column1')
    column2 = req_data.get('column2')

    if not column1 or not column2:
        return jsonify({'error': 'Both column1 and column2 are required'}), 400

    data = eval(dataset.data) 
    try:
        column1_index = dataset.data[0].index(column1)
        column2_index = dataset.data[0].index(column2)
    except ValueError:
        return jsonify({'error': 'One or both columns not found'}), 400

    plot_data = [{'x': row[column1_index], 'y': row[column2_index]} for row in data[:5]]

    return jsonify(plot_data), 200

if __name__ == '__main__':
    app.run(debug=True)
