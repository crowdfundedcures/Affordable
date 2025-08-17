import os
import json

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

# get the average value from the response (dictionary) which might contain other info as well
def parse_response(response_text: str) -> float|None:
    # Extract JSON content between ```json and ``` markers
    try:
        if "```json" in response_text:
            # Handle case where response is wrapped in ```json ... ```
            json_content = response_text.split("```json")[1].split("```")[0].strip()
        else:
            # Try to find just a JSON object in the text
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}')
            if start_idx != -1 and end_idx != -1:
                json_content = response_text[start_idx:end_idx+1].strip()
            else:
                raise ValueError("Could not find JSON content in response")
                
        print(f"Extracted JSON content: {json_content}")
        response_dict = json.loads(json_content)
        response_string = response_dict["average"]
        response_string = response_string.replace(",", "")
        response_numeric = float(response_string)
        # response_int = int(round(response_numeric))
        return response_numeric  # could be float, as in QALY
    except Exception as e:
        print(f"Error parsing response: {e}")
        print(f"Original response: {response_text}")
        return None

def get_global_patient_population(disease_name: str, reference_drug_name: str, replacement_drug_name: str):
    prompt = f"""Search for the patient population in the United States of {disease_name} 
                 and provide the answer as the minimum, maximum, and average patient population in json format
                 as a dictionary with the following structure: """ + \
                 """```json {"minimum": "0", "maximum": "0", "average": "0"}```
                 where "0" stands for the minimum, maximum, and average patient population in the US in individuals.
                 Make sure to provide the answer in the specified json format; do not provide ranges within individual values.""" # TODO
    response_text = send_request(prompt)
    value = parse_response(response_text)
    return value, response_text

def get_cost_difference(disease_name: str, reference_drug_name: str, replacement_drug_name: str):
    prompt = f"""Search for the costs of {reference_drug_name} and {replacement_drug_name} 
                 per patient per year in the US in US dollars and provide the cost difference
                 (cost of {reference_drug_name} - cost of {replacement_drug_name}) in US dollars
                 and provide the answer as the minimum, maximum, and average cost difference in US dollars in json format
                 as a dictionary with the following structure: """ + \
                 """```json {"minimum": "0", "maximum": "0", "average": "0"}```
                 where "0" stands for the minimum, maximum, and average cost difference in US dollars.
                 Make sure to provide the answer in the specified json format; do not provide ranges within individual values.""" # TODO
    response_text = send_request(prompt)
    value = parse_response(response_text)
    return value, response_text


def get_estimated_qaly_impact(disease_name: str, reference_drug_name: str, replacement_drug_name: str):
    prompt = f"""Search for estimated QALY impact of treating the {disease_name} with {reference_drug_name} 
                 for the US population and provide the answer as the minimum, maximum, and average QALY impact in json format
                 as a dictionary with the following structure: """ + \
                 """```json {"minimum": "0", "maximum": "0", "average": "0"}```
                 where "0" stands for the minimum, maximum, and average QALY impact (in years of life).
                 Make sure to provide the answer in the specified json format; do not provide ranges within individual values.""" # TODO
    response = send_request(prompt)
    value = parse_response(response)
    return value, response


def get_annual_cost(disease_name: str, reference_drug_name: str, replacement_drug_name: str):
    prompt = f"""Search for annual cost of a drug {reference_drug_name} per patient per year in the US 
                 in US dollars and provide the answer as the minimum, maximum, and average annual cost in json format
                 as a dictionary with the following structure: """ + \
                 """```json {"minimum": "0", "maximum": "0", "average": "0"}```""" + \
                 """where "0" stands for the minimum, maximum, and average annual cost in US dollars.
                 Make sure to provide the answer in the specified json format; do not provide ranges within individual values.""" # TODO
    response = send_request(prompt)
    value = parse_response(response)
    return value, response


def get_approval_likelihood(disease_name: str, reference_drug_name: str, replacement_drug_name: str, refs: set[str]):
    prompt = f"""Estimate approval likelihood of a drug {replacement_drug_name} for the {disease_name} in the US 
                 based on the following references: {refs}
                 and provide the answer as the average approval likelihood in json format
                 as a dictionary with the following structure: """ + \
                 """```json {"average": "0"}```""" + \
                 """where "1" stands for the minimal approval likelihood and 10 for the maximum approval likelihood.
                 Make sure to provide the answer in the specified json format; do not provide ranges within individual values.""" # TODO
    response = send_request(prompt)
    value = parse_response(response)
    return value, response
