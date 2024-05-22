from flask import Flask, request, jsonify
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, Text2TextGenerationPipeline

app = Flask(__name__)

# Load model and tokenizer
model_path = './models/customerId-finetuned-text2sql-model'
model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
tokenizer = AutoTokenizer.from_pretrained(model_path)
pipeline = Text2TextGenerationPipeline(model=model, tokenizer=tokenizer)

@app.route('/generate_sql', methods=['POST'])
def generate_sql():
    data = request.json
    question = data.get('question')
    if not question:
        return jsonify({"error": "No question provided"}), 400
    
    sql_query = pipeline(question, max_length=80, num_beams=4)
    return jsonify({"question": question, "sql_query": sql_query[0]['generated_text']})


if __name__ == '__main__':
    app.run(debug=True)
