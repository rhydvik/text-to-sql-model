import json
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, Trainer, TrainingArguments
from datasets import Dataset, load_dataset

# Load your dataset
with open('data/dataWithSchema.json', 'r') as f:
    data = json.load(f)

# Assuming data is structured as a list of dictionaries under a 'questions' key
questions = data["questions"]

# Prepare dataset for the model
def preprocess_data(examples):
    # Combining question and schema into a single input string
    inputs = ["question: " + example['question'] + " schema: " + example['schema'] for example in examples]
    model_inputs = tokenizer(inputs, max_length=512, truncation=True, padding="max_length", return_tensors="pt")
    
    # Assuming SQL queries are the labels
    labels = [example['sql_query'] for example in examples]
    labels = tokenizer(labels, max_length=512, truncation=True, padding="max_length", return_tensors="pt").input_ids
    
    # Replace padding token id's in the labels by -100 so that it's ignored by the loss function
    labels = [[(label if label != tokenizer.pad_token_id else -100) for label in label_example] for label_example in labels]
    
    model_inputs["labels"] = labels
    return model_inputs

# Load tokenizer and model
model_path = 'gaussalgo/T5-LM-Large-text2sql-spider'
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForSeq2SeqLM.from_pretrained(model_path)

# Convert data to datasets.Dataset object
dataset = Dataset.from_dict(preprocess_data(questions))

# Split dataset into train and validation sets
train_test_split = dataset.train_test_split(test_size=0.1)
train_dataset = train_test_split['train']
eval_dataset = train_test_split['test']

# Define training arguments
training_args = TrainingArguments(
    output_dir='./results',          # Output directory
    evaluation_strategy="epoch",     # Evaluation is done at the end of each epoch
    learning_rate=2e-5,              # Defines learning rate
    per_device_train_batch_size=16,  # Batch size for training
    per_device_eval_batch_size=64,   # Batch size for evaluation
    num_train_epochs=3,              # Number of epochs
    weight_decay=0.01,               # Strength of weight decay
    save_total_limit=3,              # Only last 3 models are saved.
    save_steps=10,                   # Save checkpoint every 10 steps
    logging_dir='./logs',            # Directory for storing logs
    logging_steps=10,                # Log every 10 steps
)

# Initialize the Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    tokenizer=tokenizer,
)

# Train the model
trainer.train()

# Save the model
trainer.save_model('./models/text2sql-trained-model')

print("Model training complete and model saved to './models/text2sql-trained-model'")
