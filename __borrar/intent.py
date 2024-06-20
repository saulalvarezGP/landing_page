
import os
os.environ['CURL_CA_BUNDLE'] = ''
from transformers import pipeline


def main():
    # Load a pre-trained model and a default tokenizer from Hugging Face
    nlp = pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english")

    # Define the text to analyze
    text = "Can you help me book a flight?"

    # Use the model to predict the intent
    result = nlp(text)

    # Print the result
    print(result)

if __name__=='__main__':
    main()