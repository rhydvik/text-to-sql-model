from flask import Flask, jsonify,request
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, Text2TextGenerationPipeline
import pymysql
from question_generator import generate_questions

app = Flask(__name__)

model_path = 'gaussalgo/T5-LM-Large-text2sql-spider'
model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
tokenizer = AutoTokenizer.from_pretrained(model_path)

# trained model and tokenizer
tuned_model_path = './models/text2sql-trained-model'
tuned_model = AutoModelForSeq2SeqLM.from_pretrained(tuned_model_path)
tuned_tokenizer = AutoTokenizer.from_pretrained(tuned_model_path)
pipeline = Text2TextGenerationPipeline(model=tuned_model, tokenizer=tuned_tokenizer)

def translate_to_sql(user_query):
    input_text = " ".join(["question:", user_query])
    model_inputs = tokenizer(input_text, return_tensors="pt")
    outputs = model.generate(**model_inputs, max_new_tokens=100)

    output_text = tokenizer.batch_decode(outputs, skip_special_tokens=True)[0]
    return output_text

def translate_with_tunde_model(user_query, schema):
    input_text = f"question: {user_query} schema: {schema}"
    model_inputs = tuned_tokenizer(input_text, return_tensors="pt")
    outputs = tuned_model.generate(**model_inputs, max_new_tokens=100)

    print(input_text, 'input text')

    output_text = tuned_tokenizer.batch_decode(outputs, skip_special_tokens=True)[0]
    return output_text

def execute_query(sql_query):
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='root1234',
            database='dev_flexcards'
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        conn.close()
        return {'results': results}
    except Exception as e:
        return {'error': str(e)}, 500


@app.route('/translate', methods=['POST'])
def translate():
    user_query = request.json['query']
    sql_query = translate_to_sql(user_query)
    return jsonify({'sql': sql_query})


@app.route('/execute', methods=['POST'])
def execute():
    user_query = request.json['query']
    results = execute_query(user_query)
    print(results)
    return jsonify({'sql': 'sql_query'})

@app.route('/query', methods=['POST'])
def process_query():
    user_query = request.json['query']
    try:
        print(user_query)
        sql_query = translate_to_sql(user_query)
        print(sql_query)
        results = execute_query(sql_query)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/generate_sql', methods=['POST'])
def generate_sql():
    data = request.json
    question = data.get('question')
    if not question:
        return jsonify({"error": "No question provided"}), 400
    
    schema = "\"cards\" \"id\" varchar, \"name\" varchar, \"userId\" varchar, \"issuer\" varchar, \"cardNumber\" varchar, \"type\" varchar, \"binCheckerResponse\" json, \"pgResponse\" json, \"cardStatus\" varchar, \"whitelistingStatus\" varchar, \"pennyAmount\" float, \"pennyAmountUpdated\" float, \"accountNumber\" varchar, \"upiId\" varchar, \"statusChangeReason\" varchar, \"mobile\" varchar, \"isHidden\" tinyint, \"isDisabled\" tinyint, \"tokenised\" tinyint, \"transaction\" tinyint, \"transactionId\" varchar, \"approvedMode\" varchar, \"createdAt\" timestamp, \"tokenRemoved\" tinyint, \"attempts\" varchar, \"updatedAt\" timestamp, \"approvedByAdmin\" tinyint, \"accountNumberUpdated\" varchar"
    sql_query = translate_with_tunde_model(question, schema)
    return jsonify({"sql_query": sql_query})

# def format_schema(table_name, columns):
#     schema_parts = []
#     primary_keys = []  # Assuming primary keys are defined; adjust logic as needed
#     foreign_keys = []  # Adjust logic to include foreign key relationships if applicable

#     for column_name, data_type in columns:
#         schema_parts.append(f'"{column_name}" {data_type}')
#         # Example logic to append primary and foreign keys, needs specific conditions

#     schema_formatted = f'"{table_name}" ' + ", ".join(schema_parts)
#     if primary_keys:
#         schema_formatted += f', primary key: {", ".join(primary_keys)}'
#     if foreign_keys:
#         schema_formatted += f', foreign_key: {", ".join(foreign_keys)}'
#     schema_formatted += ' [SEP]'

#     return schema_formatted


@app.route('/generate_logs_questions')
def log_question():
    table_name = 'logs'
    columns = [
        ('id', 'varchar'),('action', 'varchar'), ('status', 'varchar'),
        ('cardId', 'varchar')
    ]

    questions = generate_questions(table_name, columns, condition_column="status", condition_value="IN_REVIEW")
    return jsonify({"questions": questions})

@app.route('/generate_questions')
def questions_endpoint():
    # data = request.json
    table_name = 'cards'
    columns = [
        ('id', 'varchar'), ('name', 'varchar'), ('userId', 'varchar'), 
        ('issuer', 'varchar'), ('cardNumber', 'varchar'), ('type', 'varchar'), 
        ('binCheckerResponse', 'json'), ('pgResponse', 'json'), 
        ('cardStatus', 'varchar'), ('whitelistingStatus', 'varchar'), 
        ('pennyAmount', 'float'), ('pennyAmountUpdated', 'float'), 
        ('accountNumber', 'varchar'), ('upiId', 'varchar'), 
        ('statusChangeReason', 'varchar'), ('mobile', 'varchar'), 
        ('isHidden', 'tinyint'), ('isDisabled', 'tinyint'), 
        ('tokenised', 'tinyint'), ('transaction', 'tinyint'), 
        ('transactionId', 'varchar'), ('approvedMode', 'varchar'), 
        ('createdAt', 'timestamp'), ('tokenRemoved', 'tinyint'), 
        ('attempts', 'varchar'), ('updatedAt', 'timestamp'), 
        ('approvedByAdmin', 'tinyint'), ('accountNumberUpdated', 'varchar')
    ]
    
    
    if not table_name or not columns:
        return jsonify({"error": "Missing table name or columns"}), 400

    questions = generate_questions(table_name, columns, condition_column='cardStatus', condition_value='in_progress')
    return jsonify({"questions": questions})

if __name__ == '__main__':
    app.run(debug=True)
