import openai

# Set your API key here or ensure it is set in your environment variables
api_key = 'API_KEY_HERE_OR_ENV_VAR'

# Initialize the OpenAI client
openai.api_key = api_key

# Define your model ID (ensure it matches your actual model ID)
model_id = "MODEL_ID_HERE"

# Define the base prompt
base_prompt = "The input text below is a publication in Reddit from a user. Extract the videogame names mentioned in it. Return only a python style list with the videogames names in lower case as strings items. If you don't find any videogame name, return an empty list. Input text:"

def llm_response(user_input):
    # Combine the base prompt with the user input
    prompt = f"{base_prompt} {user_input}"
    
    # Create the chat completion request
    response = openai.ChatCompletion.create(
        model=model_id,
        messages=[
            {"role": "system", "content": "Chatbot that identifies videogame names in text and only returns a python style list with the identified games in lower case letters."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.6,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    
    # Return the response content
    return response.choices[0].message.content



def llm_response1(user_input):
    # Combine the base prompt with the user input
    prompt = f"{base_prompt} {user_input}"
    
    # Create the chat completion request
    response = openai.chat.completions.create(
        model=model_id,
        messages=[
            {"role": "system", "content": "Chatbot that identifies videogame names in text and only returns a python style list with the identified games in lower case letters."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.6,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    # Return the response content
    return response.choices[0].message.content

# Example usage:
user_input = "should i get rocket league on pc or xbox go to rrocketleague and witness the likes of  https  gfycatcomshamelessinformalbats League of legends is awesome but I hate fall guys"
response = llm_response1(user_input)
print(response)
