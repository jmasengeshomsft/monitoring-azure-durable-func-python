import azure.functions as func
import azure.durable_functions as df
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.trace import SpanKind
import numpy as np
import time

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
        # tasks = [
        #     context.call_activity("hello", f"City {i}")
        #     for i in range(1, 101)
        # ]

        tasks = [
            context.call_activity("heavy_computation")
            for _ in range(20)  # Adjust the number of tasks to increase load
        ]

        # Wait for all tasks to complete
        results = yield context.task_all(tasks)
        span.set_attribute("orchestrator.results", results)

        return results


# Activity function
@myApp.activity_trigger(input_name="city")
def hello(city: str):
    # Start tracing for the activity function
    with tracer.start_as_current_span("hello", kind=SpanKind.INTERNAL) as span:
        span.set_attribute("activity.city", city)
        result = f"Hello {city}"
        span.set_attribute("activity.result", result)

        return result
    
# Activity function
@myApp.activity_trigger(input_name="limit")
def calculate_primes(limit: int):
    with tracer.start_as_current_span("calculate_primes", kind=SpanKind.INTERNAL) as span:
        primes = []
        for num in range(2, limit + 1):
            is_prime = True
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    is_prime = False
                    break
            if is_prime:
                primes.append(num)
        span.set_attribute("activity.primes_count", len(primes))
        return primes
    

# Activity function
@myApp.activity_trigger(input_name="input")
def heavy_computation(input: str):
    with tracer.start_as_current_span("heavy_computation", kind=SpanKind.INTERNAL) as span:
        print("Starting heavy computation...")

        # Allocate a smaller matrix (approx 0.25GB RAM usage)
        size = 6250  # Adjust this value if needed (size * size * 8 bytes ≈ 0.25GB for float64)
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