import os

from openai import OpenAI


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL")
assert OPENAI_API_KEY
assert OPENAI_MODEL


def send_request(prompt: str) -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "user", "content": prompt},
        ]
    )
    response_text = response.choices[0].message.content
    print(f"OpenAI: response ID: {response.id}, model used: {response.model}, responce: {response_text}")
    return response_text


def get_global_patient_population(disease_name, reference_drug_name, replacement_drug_name):
    prompt = f'' # TODO
    response_text = send_request(prompt)
    value = 'parsed value' # TODO parse response
    return value, response_text


def get_cost_difference(disease_name, reference_drug_name, replacement_drug_name):
    prompt = f'' # TODO
    response = send_request(prompt)
    value = 'parsed value' # TODO parse response
    return value, response


def get_estimated_qaly_impact(disease_name, reference_drug_name, replacement_drug_name):
    prompt = f'' # TODO
    response = send_request(prompt)
    value = 'parsed value' # TODO parse response
    return value, response


def get_annual_cost(disease_name, reference_drug_name, replacement_drug_name):
    prompt = f'' # TODO
    response = send_request(prompt)
    value = 'parsed value' # TODO parse response
    return value, response
