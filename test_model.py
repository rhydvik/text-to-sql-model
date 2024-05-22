import json
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

def load_model(model_path):
    model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model.eval()  # Ensure the model is in evaluation mode
    return model, tokenizer

def load_test_data(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data['questions']

def generate_sql_predictions(model, tokenizer, test_data):
    predictions = []
    for item in test_data:
        input_text = f"question: {item['question']} schema: {item['schema']}"
        inputs = tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True, padding="max_length")
        outputs = model.generate(**inputs)
        predicted_query = tokenizer.decode(outputs[0], skip_special_tokens=True)
        predictions.append(predicted_query)
    return predictions

def evaluate_predictions(predictions, test_data):
    correct = 0
    for predicted, actual in zip(predictions, test_data):
        print(f"Predicted: {predicted}, Actual: {actual['sql_query']}")
        if predicted.strip().lower() == actual['sql_query'].strip().lower():
            correct += 1
    accuracy = correct / len(test_data)
    return accuracy

def main():
    model_path = './models/text2sql-trained-model'
    test_data_path = 'data/logs_testing_data.json'  

    # Load the model and tokenizer
    model, tokenizer = load_model(model_path)
    
    # Load the test data
    test_data = load_test_data(test_data_path)
    
    # Generate SQL predictions
    predictions = generate_sql_predictions(model, tokenizer, test_data)
    
    # Evaluate the predictions
    accuracy = evaluate_predictions(predictions, test_data)
    print(f"Model Accuracy: {accuracy:.2%}")

if __name__ == "__main__":
    main()
