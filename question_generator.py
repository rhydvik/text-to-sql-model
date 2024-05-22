from transformers import T5ForConditionalGeneration, T5Tokenizer
from google_pegasus import pegasus_paraphrase
from sql_template import TEMPLATES, format_schema


def fill_template(template, table, column=None):
    filled_question = template['question'].format(table=table, column=column if column else '[column]')
    filled_sql = template['sql'].format(table=table, column=column if column else '[column]')
    return filled_question, filled_sql


def paraphrase_question(question, model_name="t5-base", num_paraphrases=3):
    from transformers import T5ForConditionalGeneration, T5Tokenizer
    tokenizer = T5Tokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)

    # Adjusting the prompt for clarity
    prompt = f"paraphrase: {question} </s>"

    inputs = tokenizer(prompt, return_tensors="pt", padding=True, truncation=True, max_length=512)
    outputs = model.generate(
        inputs['input_ids'], 
        max_length=60, 
        num_return_sequences=num_paraphrases,
        num_beams=10, 
        temperature=1.0)

    paraphrases = [tokenizer.decode(output_id, skip_special_tokens=True) for output_id in outputs]
    return paraphrases



def generate_paraphrased_questions_and_queries(original_questions_and_queries):
    paraphrased_questions_and_queries = []
    for question, sql_query in original_questions_and_queries:
        print(question, 'question')
        paraphrases = paraphrase_question(question, model_name="t5-base", num_paraphrases=3)
        print(paraphrases, 'para phrases')
        for paraphrase in paraphrases:
            paraphrased_questions_and_queries.append((paraphrase, sql_query))
    
    return paraphrased_questions_and_queries



def generate_questions2(table_name, columns):
    questions_and_queries = []
    
    for column_name, data_type in columns:
        if data_type in ['varchar', 'timestamp']:
            question, sql = fill_template(TEMPLATES['select_basic'], table_name, column_name)
            questions_and_queries.append((question, sql))
        
        elif data_type == 'float':
            question, sql = fill_template(TEMPLATES['average'], table_name, column_name)
            questions_and_queries.append((question, sql))
            
        elif data_type == 'tinyint':
            question, sql = fill_template(TEMPLATES['conditional'], table_name, column_name)
            questions_and_queries.append((question, sql))
    
    complex_question, complex_sql = fill_template(TEMPLATES['complex_condition'], table_name, 'pennyAmount')
    questions_and_queries.append((complex_question, complex_sql))

    expanded_questions_and_queries = []
    for question, sql in questions_and_queries:
        paraphrases = pegasus_paraphrase(question)
        for paraphrase in paraphrases:
            expanded_questions_and_queries.append((paraphrase, sql))
    
    return expanded_questions_and_queries

def select_template(column_name, data_type):
    # Example logic to choose conditional select based on data type or column names
    if data_type == 'varchar' and "status" in column_name:
        return TEMPLATES['conditional_select']
    elif data_type in ['int', 'float'] and "age" in column_name:
        return TEMPLATES['aggregate_avg']
    elif data_type in ['int', 'bigint']:
        return TEMPLATES['aggregate_count']
    else:
        return TEMPLATES['select_basic']



def generate_questions(table_name, columns, condition_column=None, condition_value=None):
    questions_and_queries = []
    schema = format_schema(table_name, columns)
    
    for column_name, data_type in columns:
        template = select_template(column_name, data_type)
        
        # Prepare the question and SQL query using the template and condition
        if template == TEMPLATES['conditional_select'] and condition_column and condition_value:
            question = template['question'].format(column=column_name, table=table_name, condition_column=condition_column, condition_value=condition_value)
            sql_query = template['sql_query'].format(column=column_name, table=table_name, condition_column=condition_column, condition_value=condition_value)
        else:
            question = template['question'].format(column=column_name, table=table_name)
            sql_query = template['sql_query'].format(column=column_name, table=table_name)
        
        questions_and_queries.append({"question": question, "schema": schema, "sql_query": sql_query})
    
    return questions_and_queries

