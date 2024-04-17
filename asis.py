import time
from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def wait_on_run(run, thread):
    while run.status == "queued" or run.status == "in_progress":
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id,
        )
        time.sleep(0.5)
    return run

def submit_and_wait_for_response(question):
    try:
        # Retrieve an existing assistant by ID
        assistant_id = "asst_mxxoMvgJP92y85zMFS9q1INB"

        # Create a new thread
        thread = client.beta.threads.create()

        # Send the question to the thread
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=question
        )

        # Execute the thread with the specified assistant
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id,
        )

        # Wait for the run to complete
        run = wait_on_run(run, thread)

        # Get the response from the assistant
        messages = client.beta.threads.messages.list(thread_id=thread.id)
        last_message = messages.data[0]  
        response = last_message.content[0].text.value

        return response

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Example usage