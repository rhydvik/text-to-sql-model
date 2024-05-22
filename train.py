import json
from datasets import Dataset, load_metric
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, Trainer, TrainingArguments

# Load your dataset
with open('traindata.json', 'r') as f:
    card_dataset = json.load(f)


questions = [item[0] for item in card_dataset["questions"]]
queries = [item[1] for item in card_dataset["questions"]]

dataset_dict = {
    "question": questions,
    "query": queries
}


full_dataset = Dataset.from_dict(dataset_dict)


def preprocess_data(examples):
    inputs = [f"translate English to SQL: {q}" for q in examples["question"]]
    model_inputs = tokenizer(inputs, max_length=512, truncation=True, padding="max_length")

    # Prepare labels
    labels = tokenizer(examples["query"], max_length=512, truncation=True, padding="max_length")
    model_inputs["labels"] = [[(l if l != tokenizer.pad_token_id else -100) for l in label] for label in labels["input_ids"]]
    
    return model_inputs

tokenizer = AutoTokenizer.from_pretrained('gaussalgo/T5-LM-Large-text2sql-spider')


# Apply the preprocessing function to the dataset
tokenized_dataset = full_dataset.map(preprocess_data, batched=True)

# Split the dataset into training and evaluation sets
train_dataset, eval_dataset = tokenized_dataset.train_test_split(test_size=0.2).values()

# Define training arguments
training_args = TrainingArguments(
    output_dir='./results',
    num_train_epochs=3,
    per_device_train_batch_size=2,  # Adjust according to your GPU memory
    gradient_accumulation_steps=2,
    save_steps=500,
    evaluation_strategy='steps',
    eval_steps=500,
    logging_strategy='steps',
    logging_steps=50,
    learning_rate=3e-5,
    warmup_steps=100,
    weight_decay=0.01,
    load_best_model_at_end=True,
    metric_for_best_model="accuracy",  # Choose an appropriate metric
)

# Load the model
model = AutoModelForSeq2SeqLM.from_pretrained('gaussalgo/T5-LM-Large-text2sql-spider')

# Initialize the Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    tokenizer=tokenizer,
)

# Start training
trainer.train()

# Save the fine-tuned model
trainer.save_model('./models/customerId-finetuned-text2sql-model')

print("Model training complete and saved to ./models/finetuned-text2sql-model")
