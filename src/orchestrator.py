import ray
from ray import serve
from vllm import AsyncLLMEngine, EngineArgs, SamplingParams
import os
import uuid

# --- CONFIGURATION ---
# We use the config you wanted (Llama 3), but we add a fallback to OPT-125M
# just in case you don't have the HF_TOKEN set up yet.
MODEL_ID = os.getenv("MODEL_ID", "meta-llama/Meta-Llama-3-8B-Instruct")
# MODEL_ID = "facebook/opt-125m" # Uncomment this if Llama fails to auth

@serve.deployment(
    name="llama-3-8b",
    num_replicas=1,
    ray_actor_options={
        "num_gpus": 1, 
        "num_cpus": 4 # vLLM needs some CPU for the scheduler
    }
)
class VLLMDeployment:
    def __init__(self):
        print(f"[VLLM] Initializing Engine for {MODEL_ID} on Intel XPU...")
        
        # 1. Check for Hugging Face Token (Critical for Llama 3)
        if "meta-llama" in MODEL_ID and not os.getenv("HF_TOKEN"):
            print("WARNING: HF_TOKEN not found! Llama 3 requires authentication.")
            print("Export HF_TOKEN='your_token' in your shell before launching.")

        # 2. Configure vLLM for Aurora (Intel GPUs)
        try:
            engine_args = EngineArgs(
                model=MODEL_ID,
                # device="xpu",                # <--- THE MAGIC WORD FOR AURORA
                tensor_parallel_size=1,      # 1 GPU per replica
                gpu_memory_utilization=0.90, # Use 90% of tile memory
                max_model_len=4096,
                enforce_eager=True,          # Intel XPU often prefers eager mode for stability
                # trust_remote_code=True
            )
            
            # WORKAROUND: Aurora vLLM version expects 'enable_log_requests' in EngineArgs
            if not hasattr(engine_args, "enable_log_requests"):
                engine_args.enable_log_requests = True

            # 3. Build the Async Engine
            self.engine = AsyncLLMEngine.from_engine_args(engine_args)
            print("[VLLM] Engine Successfully Initialized!")
            
        except Exception as e:
            print(f"[VLLM] CRITICAL FAILURE: {e}")
            raise e

    async def __call__(self, request):
        # 4. Parse the Request
        # Supports both raw text (curl -d "prompt") and JSON
        if isinstance(request, str):
            prompt = request
        else:
            payload = await request.json()
            prompt = payload.get("prompt", "What is the future of supercomputing?")

        print(f"[VLLM] Received Prompt: {prompt[:30]}...")

        # 5. Define Sampling Parameters
        sampling_params = SamplingParams(
            temperature=0.7, 
            max_tokens=100,
            stop=["<|eot_id|>"]
        )
        
        # 6. Generate Stream
        request_id = str(uuid.uuid4())
        results_generator = self.engine.generate(prompt, sampling_params, request_id)

        # 7. Collect Output (Non-streaming for simplicity)
        final_text = ""
        async for request_output in results_generator:
            final_text = request_output.outputs[0].text

        return {"model": MODEL_ID, "prompt": prompt, "response": final_text}

# --- ENTRY POINT ---
if __name__ == "__main__":
    # Connect to the local cluster
    ray.init(address="auto", namespace="serve")
    
    print(f"[Orchestrator] Deploying vLLM Service for {MODEL_ID}...")
    serve.run(VLLMDeployment.bind())
    
    print("[Orchestrator] Deployment active. Waiting for requests...")
    import time
    while True:
        time.sleep(10)
