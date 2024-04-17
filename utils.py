import fitz
import json
import sqlite3
import tiktoken
import re
import pandas as pd
from PyPDF2 import PdfReader
from pdfminer.high_level import extract_text_to_fp
from io import BytesIO

prompts = {
    "EIN0306306H6": {
        "Prompt": """
        {
            "invoice_number": "str",
            "invoice_date": "str",
            "e_docu": "str",
            "buyer": "str",
            "rfc": "str",
            "incoterm": "str",
            "lumps": "int",
            "total": "float",
            "items": [
                {
                    "gross_weight": "float",
                    "net_weight": "float",
                    "description": "str",
                    "part_number": "str",
                    "quantity": "float",
                    "unit_of_measure": "str",
                    "fraction": "str",
                    "raw_material": "float",
                    "value_added": "float",
                    "total": "float",
                }
            ]
        }
    """
    },
    "EAT930158UR8": {
        "Prompt": """
        {
            "invoice_number": "str",
            "invoice_date": "str",
            "country_of_origin": "str",
            "supplier": "str",
            "total": "float",
            "items": [
                {
                    "part_number": "str",
                    "description": "str",
                    "quantity": "int",
                    "unit_of_measure": "str",
                    "cost": "float",
                    "weight": "float"
                }
            ]
        }"""
    },
    "GENERAL": {
        "Prompt": """
        {
            "invoice_number": "str",
            "invoice_date": "str",
            "country_of_origin": "str",
            "supplier": "str",
            "total": "float",
            "items": [
                {
                    "part_number": "str",
                    "description": "str",
                    "quantity": "int",
                    "unit_of_measure": "str",
                    "cost": "float",
                    "weight": "float"
                }
            ]
        }"""
    }
}

def convert_pdf_to_text(pdf_path):
    output_buffer = BytesIO()
    with open(pdf_path, 'rb') as pdf_file:
        extract_text_to_fp(pdf_file, output_buffer, output_type='tag')
        text_content = output_buffer.getvalue().decode('utf-8')
        print(text_content)
    return text_content


def search_word(text, word):
    # Using \b to define whole word boundaries
    # and re.IGNORECASE to ignore case sensitivity
    # The expression (?:(?<=\S)\S*|\b) before and after the word
    # allows handling cases where the text is joined together without spaces
    pattern = r"(?:(?<=\S)\S*|\b){}(?=\S*\S(?=\S)|\b)".format(re.escape(word))
    return re.findall(pattern, text, re.IGNORECASE)


def get_prompt(rfc):
    """
    Retrieves the prompt for a given RFC.
    """
    rfc = rfc.upper()  # Ensure RFC is in uppercase for uniformity
    # Return the prompt for the given RFC if it exists, otherwise return the general prompt
    return prompts.get(rfc, prompts["GENERAL"])["Prompt"]


def search_RFC_in_text(text):
    """
    Searches for an RFC in the given text and returns the corresponding prompt if found.
    """
    rfc_list = ["EIN0306306H6", "EAT930128UR6"]
    for rfc in rfc_list:
        # Check if the RFC is found in the text
        if search_word(text, rfc):
            print(f"RFC {rfc} encontrado")
            return get_prompt(rfc), rfc
    # Return the general prompt if no RFC is found
    print("RFC no encontrado")
    return get_prompt("GENERAL"),"EIN0306306H6"


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding_name = "cl100k_base"
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def reader(pdf_path):
    text=convert_pdf_to_text(pdf_path)
    numTokens= num_tokens_from_string(text)
    numTokens+50
    return text,numTokens


def extract_text_from_pdf(pdf_path):
    text=""
    pdf_reader=PdfReader(pdf_path)
    for page in pdf_reader.pages:
        text+=page.extract_text()
    numTokens= num_tokens_from_string(text)
    numTokens+50
    return text,numTokens


def execute_sql(sql, parameters=None):
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    try:
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    finally:
        conn.close()

# Función para insertar los datos en la base de datos
def insert_data_to_db(json_data):
    sql_invoice, sql_items = parse_json_to_sql(json_data)

    if sql_invoice and sql_items:
        execute_sql(sql_invoice)
        for sql_item in sql_items:
            execute_sql(sql_item)

def normalize_keys(data):
    """Convierte todas las claves del diccionario a minúsculas."""
    if isinstance(data, dict):
        return {k.lower(): normalize_keys(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_keys(v) for v in data]
    else:
        return data
    
    
def parse_json_to_sql(json_data):
    try:
        # Convierte la cadena JSON a un diccionario
        data = normalize_keys(json.loads(json_data))

        # Extrae los datos de la factura
        invoice_number = data.get("invoice_number")
        invoice_date = data.get("invoice_date")
        country_of_origin = data.get("country_of_origin")
        supplier = data.get("supplier")
        total = data.get("total")
        if total.count("$") > 1:
            total = total.replace("$", "", 1)
        elif total.count("$") == 0:
            total = total

        # Prepara la instrucción SQL para la tabla Invoice
        sql_invoice = f"INSERT INTO Invoice (InvoiceNumber, InvoiceDate, CountryOfOrigin, Supplier,Total) VALUES ('{invoice_number}', '{invoice_date}', '{country_of_origin}', '{supplier}','{total}');"

        # Prepara las instrucciones SQL para la tabla Item
        sql_items = []
        for item in data.get("items", []):
            part_number = item.get("part_number")
            description = item.get("description")
            quantity = item.get("quantity")
            unit_of_measure = item.get("unit_of_measure")
            cost = item.get("cost").replace("$", "")  # Elimina el signo de dólar si está presente
            weight = item.get("weight")

            sql_item = f"INSERT INTO Item (InvoiceNumber, PartNumber, Description, Quantity, UnitOfMeasure, Cost, Weight) VALUES ('{invoice_number}', '{part_number}', '{description}', {quantity}, '{unit_of_measure}', {cost}, {weight});"
            sql_items.append(sql_item)

        return sql_invoice, sql_items

    except json.JSONDecodeError:
        print("Error al parsear el JSON" + json_data)
        return None, None
    except KeyError as e:
        print(f"Clave no encontrada en el JSON: {e}")
        return None, None
    


def convert_json_to_dataframe_invoice(invoice_dict):
    if not isinstance(invoice_dict, dict):
        return pd.DataFrame()

    # Extraer el primer elemento de cada lista para obtener los valores reales
    invoice_data = {key: value[0] if isinstance(value, list) else value for key, value in invoice_dict.items()}

    return pd.DataFrame([invoice_data])

def convert_json_to_dataframe_items(items_list):
    if not isinstance(items_list, list) or not all(isinstance(item, list) for item in items_list):
        return pd.DataFrame()

    # Asumimos que los ítems están en la primera lista, si la estructura es una lista de listas
    items = items_list[0] if items_list and isinstance(items_list[0], list) else items_list

    return pd.DataFrame(items)

def is_pdf_readable(pdf_path):
    try:
        with fitz.open(pdf_path) as doc:  # Open the PDF file
            text = ""
            for page in doc: 
                text += page.get_text()
            return True
    except:
        return False
    
