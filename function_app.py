import random
import azure.functions as func
import azure.durable_functions as df
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.trace import SpanKind
import numpy as np
import time
import datetime
from azure.storage.queue import (
        QueueServiceClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
from azure.data.tables import TableServiceClient, UpdateMode
import os
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
import json
import uuid
from openai import AzureOpenAI

# Configure Azure Monitor with OpenTelemetry
configure_azure_monitor()

# Initialize tracer
tracer = trace.get_tracer(__name__)

# Create a Durable Functions app
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# HTTP-triggered function with a Durable Functions client binding
@myApp.route(route="orchestrators/{functionName}")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    # Start tracing for the HTTP request
    with tracer.start_as_current_span("http_start", kind=SpanKind.SERVER) as span:
        # Extract function name from route params
        function_name = req.route_params.get("functionName")
        span.set_attribute("http.method", req.method)
        span.set_attribute("http.url", req.url)
        span.set_attribute("function.name", function_name)

        # Start a new orchestration instance
        instance_id = await client.start_new(function_name)
        span.set_attribute("durable.instance_id", instance_id)

        # Create HTTP response with instance management
        response = client.create_check_status_response(req, instance_id)
        span.set_attribute("http.status_code", response.status_code)

        return response


# Orchestrator function
@myApp.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    # Start tracing for the orchestrator
    with tracer.start_as_current_span("hello_orchestrator", kind=SpanKind.INTERNAL) as span:
        
        tasks = [
            context.call_activity("heavy_computation", input_data)
            for input_data in range(5000)  # Adjust the number of tasks to increase load
        ]

        # Wait for all tasks to complete
        results = yield context.task_all(tasks)
        span.set_attribute("orchestrator.results", results)

        return results

# Activity function
@myApp.activity_trigger(input_name="input")
def heavy_computation(input: int):
    with tracer.start_as_current_span("heavy_computation", kind=SpanKind.INTERNAL) as span:
        print("Starting heavy computation...")

        # Allocate a smaller matrix (approx 0.25GB RAM usage)
        size = 2000  # Adjust this value if needed (size * size * 8 bytes ≈ 0.25GB for float64)
        print(f"Creating a {size}x{size} matrix...")

        # Create smaller random matrices
        matrix_a = np.random.rand(size, size)
        matrix_b = np.random.rand(size, size)

        print("Performing matrix multiplication...")
        start_time = time.time()

        # Perform a heavy computation (Matrix Multiplication)
        result = np.dot(matrix_a, matrix_b)

        # Compute the sum of all elements (memory-heavy aggregation)
        result_sum = np.sum(result)

        end_time = time.time()

        print(f"Computation completed. Result sum: {result_sum}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")

        span.set_attribute("computation.result_sum", result_sum)
        span.set_attribute("computation.time_taken", end_time - start_time)

        return result_sum
    
# Function 1: Create messages from Table Storage and send to Queue
@myApp.function_name(name="create_message_timer")
@myApp.timer_trigger(schedule="0 */1 * * * *", 
              arg_name="mytimer",
              run_on_startup=False) 
def create_message_timer(mytimer: func.TimerRequest) -> None:
    """
    Timer-triggered function that reads records from Table Storage and sends messages to a Storage Queue.
    
    Args:
        mytimer (func.TimerRequest): The Timer request object.
    """
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    with tracer.start_as_current_span("create_message_timer", kind=SpanKind.SERVER) as span:

        # Use DefaultAzureCredential to authenticate with managed identity
        credential = DefaultAzureCredential()

        storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "azdurablefunc001")
        storage_table_name = os.getenv("STORAGE_TABLE_NAME", "hardwarebugs")
        storage_queue_name = os.getenv("STORAGE_QUEUE_NAME", "bugmessages")

        table_service = TableServiceClient(
            endpoint=f"https://{storage_account_name}.table.core.windows.net", 
            credential=credential
        )
        table_client = table_service.get_table_client(table_name=storage_table_name)
        queue_service = QueueServiceClient(
            account_url=f"https://{storage_account_name}.queue.core.windows.net", 
            credential=credential
        )
        queue_client = queue_service.get_queue_client(queue=storage_queue_name)

        queue_client.message_encode_policy = BinaryBase64EncodePolicy()
        queue_client.message_decode_policy = BinaryBase64DecodePolicy()


        # Load messages records whch are in 'New' status
        entities = table_client.query_entities("Status eq 'New'")
        for entity in entities:
            partition_key = entity.get("PartitionKey", "UnknownPartitionKey")
            row_key = entity.get("RowKey", "UnknownRowKey")
            bug_id = entity.get("BugId", "UnknownBugId")
            joke = entity.get("Joke", "UnknownJoke")

            # create a message and send to queue
            message = {
                "PartitionKey": partition_key,
                "RowKey": row_key,
                "BugId": bug_id,
                "Joke": joke
            }

            # Encode the message as JSON and send to the queue
            message = json.dumps(message)
            message_bytes =  message.encode('utf-8')
            queue_client.send_message(queue_client.message_encode_policy.encode(content=message_bytes))
            trace.get_current_span().add_event("Sent message", {"message": message})

            span.set_attribute("entity.partition_key", partition_key)
            span.set_attribute("entity.row_key", row_key)
            span.add_event("Message sent to queue", {"message": message})

        span.add_event("Python timer trigger function ran", {"utc_timestamp": utc_timestamp})

# Function 2: Process messages from Queue and update Table Storage
@myApp.function_name("process_message")
@myApp.queue_trigger(arg_name="msg", queue_name="bugmessages", connection="AZURE_STORAGE_CONNECTION_STRING")
async def process_message(msg: func.QueueMessage):
    """
    Queue-triggered function that processes messages and updates Table Storage.
    
    Args:
        msg (func.QueueMessage): The queue message object.
    """
    with tracer.start_as_current_span("process_message", kind=SpanKind.SERVER) as span:
        try:
            # Use DefaultAzureCredential to authenticate with managed identity
            credential = DefaultAzureCredential()

            storage_table_name = os.getenv("STORAGE_TABLE_NAME", "hardwarebugs")
            storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "azdurablefunc001")

            table_service = TableServiceClient(
                endpoint=f"https://{storage_account_name}.table.core.windows.net",
                credential=credential
            )
            table_client = table_service.get_table_client(table_name=storage_table_name)

            message_content = msg.get_body().decode('utf-8')

            # Log the message content using OpenTelemetry
            trace.get_current_span().add_event("Processing message", {"message_content": message_content})

            # Extract PartitionKey and RowKey from the message
            message_dict = json.loads(message_content)
            partition_key = message_dict.get("PartitionKey")
            row_key = message_dict.get("RowKey")
            bug_id = message_dict.get("BugId")
            joke = message_dict.get("Joke")
            span.set_attribute("message.bug_id", bug_id)

            # Retrieve the entity from Table Storage
            entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

            # Update the entity as "processed"
            entity["Status"] = "Processed"

            # explain joke
            time.sleep(10)
            joke_context = summarize_text(joke)
            entity["JokeContext"] = joke_context
            entity["Updated_at"] = datetime.datetime.utcnow().isoformat()

            table_client.update_entity(entity, mode=UpdateMode.REPLACE)
            trace.get_current_span().add_event("Updated entity", {
                "partition_key": partition_key,
                "row_key": row_key,
                "status": "Processed",
                "joke_context": joke_context
            })


            # call the method to call open ai and summarize the text the text below
            # text = "I recently discovered I could make fudge with just chocolate chips, sweetened condensed milk, vanilla extract, and a thick pot on slow heat. I tried it with dark chocolate chunks and I tried it with semi-sweet chocolate chips. It's better with both kinds. It comes out pretty bad with just the dark chocolate. The best add-ins are crushed almonds and marshmallows -- what you get from that is Rocky Road. It takes about twenty minutes from start to fridge, and then it takes about six months to work off the twenty pounds you gain from eating it. All things in moderation, friends. All things in moderation."
            
            
           # summarize_text(text)

            span.set_attribute("entity.partition_key", partition_key)
            span.set_attribute("entity.row_key", row_key)
            span.add_event("Entity updated", {"partition_key": partition_key, "row_key": row_key})
        except Exception as e:
            span.record_exception(e)
            raise e

# Function 4: Generate random messages and store in Table Storage
@myApp.function_name(name="generate_random_messages")
@myApp.timer_trigger(schedule="0 */1 * * * *", 
              arg_name="mytimer",
              run_on_startup=False) 
def generate_random_messages(mytimer: func.TimerRequest) -> None:
    """
    Timer-triggered function that generates 500 unique and random messages per minute and stores them in Table Storage.
    
    Args:
        mytimer (func.TimerRequest): The Timer request object.
    """
    with tracer.start_as_current_span("generate_random_messages", kind=SpanKind.INTERNAL) as span:
        try:
            # Use DefaultAzureCredential to authenticate with managed identity
            credential = DefaultAzureCredential()

            storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "azdurablefunc001")
            storage_table_name = os.getenv("STORAGE_TABLE_NAME", "hardwarebugs")
            messages_per_minute = int(os.getenv("MESSAGES_PER_MINUTE", "5"))

            table_service = TableServiceClient(
                endpoint=f"https://{storage_account_name}.table.core.windows.net",
                credential=credential
            )
            table_client = table_service.get_table_client(table_name=storage_table_name)

            batch_operations = []
            partition_key = str(uuid.uuid4())

            for _ in range(messages_per_minute):
                # partition_key = str(uuid.uuid4())
                row_key = str(uuid.uuid4())
                bug_id = str(random.randint(1, 99999))
                joke = generate_random_joke()
                span.set_attribute("message.bug_id", bug_id)

                # Create a message and store in Table Storage
                message = {
                    "PartitionKey": partition_key,
                    "RowKey": row_key,
                    "BugId": bug_id,
                    "Status": "New",
                    "Joke": joke,
                    "Created_at": datetime.datetime.utcnow().isoformat()
                }

                batch_operations.append(("create", message))
                span.add_event("Prepared message for batch", {"message": json.dumps(message)})

            table_client.submit_transaction(batch_operations)
            span.add_event("Batch operation completed", {"batch_size": len(batch_operations)})
            time.sleep(10)
        except Exception as e:
            span.record_exception(e)
            raise e
        
def summarize_text(text: str) -> str:
    """
    Summarize the given text using Azure OpenAI service.

    Args:
        text (str): The text to be summarized.

    Returns:
        str: The summarized text.
    """
    with tracer.start_as_current_span("AzureOpenAI", kind=SpanKind.CLIENT) as span:  # Mark this as a dependency call
        # Use DefaultAzureCredential to authenticate with managed identity
        credential = DefaultAzureCredential()

        # Assuming the OpenAI endpoint and deployment name are set as environment variables
        openai_endpoint = os.getenv("OPENAI_ENDPOINT")
        openai_deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")

        token_provider = get_bearer_token_provider(
            credential, "https://cognitiveservices.azure.com/.default"
        )

        client = AzureOpenAI(
            azure_endpoint=openai_endpoint,
            azure_ad_token_provider=token_provider,
            api_version= os.getenv("API_VERSION_GA", "2024-02-15-preview"),
        )

        prompt = f"Given this Kubernetes sarcastic joke, explain what the context is. Why do you think this joke was choose and what are we poking about Kubernetes complexity? Here is the joke: {text}"
        span.set_attribute("openai.prompt", prompt)

        response = client.chat.completions.create(
            model=openai_deployment_name,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )

        summarized_text = response.choices[0].message.content
        span.set_attribute("openai.summarized_text", summarized_text)

        return summarized_text
    

def generate_random_joke():
    """
    Generate a random joke using Azure OpenAI service.
    Returns:
        str: A random joke.
    """
    with tracer.start_as_current_span("AzureOpenAI", kind=SpanKind.CLIENT) as span:  # Mark this as a dependency call
        # Use DefaultAzureCredential to authenticate with managed identity
        credential = DefaultAzureCredential()

        # Assuming the OpenAI endpoint and deployment name are set as environment variables
        openai_endpoint = os.getenv("OPENAI_ENDPOINT")
        openai_deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")

        token_provider = get_bearer_token_provider(
            credential, "https://cognitiveservices.azure.com/.default"
        )

        client = AzureOpenAI(
            azure_endpoint=openai_endpoint,
            azure_ad_token_provider=token_provider,
            api_version= os.getenv("API_VERSION_GA", "2024-02-15-preview"),
        )

        prompt = "Give me a random and sarcastic joke about Kubernetes complexity. Something like: I would tell you a joke about Kubernetes, but it would probably require 5 different microservices to run."
        span.set_attribute("openai.prompt", prompt)

        response = client.chat.completions.create(
            model=openai_deployment_name,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )

        joke_text = response.choices[0].message.content
        span.set_attribute("openai.response", joke_text)

        return  joke_text
# Run the Durable Functions app