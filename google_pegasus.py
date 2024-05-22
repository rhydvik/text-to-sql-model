from transformers import PegasusForConditionalGeneration, PegasusTokenizer

model_name = 'google/pegasus-xsum'
tokenizer = PegasusTokenizer.from_pretrained(model_name)
model = PegasusForConditionalGeneration.from_pretrained(model_name)

def pegasus_paraphrase(question):
    inputs = tokenizer.encode("paraphrase: " + question, return_tensors="pt", max_length=512, truncation=True)
    outputs = model.generate(inputs, max_length=60, num_return_sequences=20, num_beams=20, temperature=1.5)
    paraphrases = [tokenizer.decode(output_id, skip_special_tokens=True) for output_id in outputs]
    return paraphrases

# question = "What is the average pennyAmount in the cards table?"
# paraphrases = pegasus_paraphrase(question)
# for paraphrase in paraphrases:
#     print(paraphrase)
