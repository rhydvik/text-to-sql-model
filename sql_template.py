TEMPLATES = {
    'select_basic': {
        'question': "What are the {column} in {table}?",
        'sql_query': "SELECT {column} FROM {table};"
    },
    'conditional_select': {
        'question': "List all {column} from {table} where {condition_column} = '{condition_value}'.",
        'sql_query': "SELECT {column} FROM {table} WHERE {condition_column} = '{condition_value}';"
    },
    'aggregate': {
        'average': {
            'question': "What is the average {column} in {table}?",
            'sql_query': "SELECT AVG({column}) FROM {table};"
        },
        'count': {
            'question': "How many {column} are in {table}?",
            'sql_query': "SELECT COUNT({column}) FROM {table};"
        },
        'min_max': {
            'question': "What are the minimum and maximum {column} in {table}?",
            'sql_query': "SELECT MIN({column}), MAX({column}) FROM {table};"
        }
    },
    'join': {
        'inner_join': {
            'question': "Show details from {table_a} and {table_b} based on {join_column}.",
            'sql_query': "SELECT * FROM {table_a} INNER JOIN {table_b} ON {table_a}.{join_column} = {table_b}.{join_column};"
        }
    },
    'order_by': {
        'question': "Order the {column} in {table} by {order_column}.",
        'sql_query': "SELECT {column} FROM {table} ORDER BY {order_column} DESC;"
    }
    # Additional templates for other types of queries can be added here
}


def format_schema(table_name, columns):
    """
    Formats the schema string by including the table name as the first element,
    followed by column definitions.
    
    Args:
    - table_name (str): The name of the table.
    - columns (list of tuples): A list where each tuple contains the column name and data type.
    
    Returns:
    - str: A formatted schema string with the table name and column details.
    """
    formatted_schema = f'"{table_name}" '
    
    column_definitions = []
    for column_name, data_type in columns:
        column_definitions.append(f'"{column_name}" {data_type}')
    
    formatted_schema += ", ".join(column_definitions)
    
    return formatted_schema
