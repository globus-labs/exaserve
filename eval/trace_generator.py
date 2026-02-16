import argparse
import yaml
import random
import pandas as pd
import numpy as np
import json
import os
import sys
from typing import Optional, Union
from dataclasses import asdict

from exp_configs import *

try:
    from transformers import AutoTokenizer
except ImportError:
    print("!!! ERROR: Please install transformers: 'pip install transformers'")
    sys.exit(1)

class TraceGenerator:
    def __init__(self, exp_config: ExpConfig):
        """
        Initialize TraceGenerator with an ExpConfig object instead of a config file path.
        
        Args:
            exp_config: ExpConfig object containing all configuration
        """
        self.exp_config = exp_config
        
        # Extract models from ExpConfig
        self.models_cfg = []
        self.model_specs = {}
        
        for model_cfg in exp_config.model_deployment_config.model_configs:
            # Match model_staging convention: local dirs use model_id with "/" -> "--"
            storage_path = exp_config.model_deployment_config.model_storage_path
            safe_name = model_cfg.model_id.replace("/", "--")
            local_path = os.path.join(storage_path, safe_name)
            tokenizer_path = local_path if os.path.isdir(local_path) else model_cfg.model_id
            model_entry = {
                model_cfg.model_id: {
                    'tensor_parallel_size': model_cfg.tensor_parallel_size,
                    'size': model_cfg.size,
                    'num_replicas': model_cfg.num_replicas,
                    'tokenizer_path': tokenizer_path,
                }
            }
            self.models_cfg.append(model_entry)
            tp = model_cfg.tensor_parallel_size
            if tp < 1:
                print(f"!!! ERROR: tensor_parallel_size for '{model_cfg.model_id}' must be a positive integer, got {tp}.")
                sys.exit(1)
            self.model_specs[model_cfg.model_id] = {
                'tensor_parallel_size': tp,
                'size': model_cfg.size,
                'tokenizer_path': tokenizer_path,
                'max_model_len': model_cfg.max_model_len,
            }
        self.seed = exp_config.job_seed
        random.seed(self.seed)
        np.random.seed(self.seed)
        
        # Multi-Tokenizer Registry
        self.tokenizers = {}
        self._load_tokenizers()

        self.prompt_bank = [] 
        self._current_prompt_path = None

    def _load_tokenizers(self):
        """
        Loads the specific tokenizer for EVERY model defined in config.
        """
        print(f">>> [GEN] Loading tokenizers for {len(self.models_cfg)} models...")
        
        # We also load a generic fallback just in case
        try:
            self.fallback_tokenizer = AutoTokenizer.from_pretrained("gpt2")
            self.fallback_tokenizer.model_max_length = 100_000_000
        except:
            self.fallback_tokenizer = None

        for entry in self.models_cfg:
            # Entry format: { "meta-llama/Meta-Llama-3-8B": { ... } }
            model_id = list(entry.keys())[0]
            
            # If the user specified a different tokenizer path in the model config, use it
            # Otherwise, assume the model_id IS the tokenizer path (standard HuggingFace behavior)
            tokenizer_path = entry[model_id].get("tokenizer_path", model_id)
            
            if model_id in self.tokenizers:
                continue

            try:
                # print(f"    Loading: {tokenizer_path}...")
                tok = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
                
                # Silence warnings and set infinite cap (we truncate manually)
                tok.model_max_length = 100_000_000 
                if hasattr(tok, "deprecation_warnings"):
                    tok.deprecation_warnings["Asking-to-pad"] = True
                
                self.tokenizers[model_id] = tok
                
            except Exception as e:
                print(f"!!! WARNING: Could not load tokenizer for {model_id}: {e}")
                print(f"    Will fall back to GPT-2 for this model.")
                if self.fallback_tokenizer:
                    self.tokenizers[model_id] = self.fallback_tokenizer

    def _load_prompt_dataset(self, path: str):
        # Load ShareGPT into self.prompt_bank
        if not path:
             return
        
        if self._current_prompt_path == path and self.prompt_bank:
            return

        if os.path.exists(path):
            print(f">>> [GEN] Loading prompts from {path}...")
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.prompt_bank = []
                for entry in data:
                    convs = entry.get('conversations', [])
                    first_prompt = next((c['value'] for c in convs if c['from'] == 'human'), None)
                    if first_prompt:
                        self.prompt_bank.append(first_prompt)
                
                print(f">>> [GEN] Indexed {len(self.prompt_bank)} raw prompts.")
                self._current_prompt_path = path
            except Exception as e:
                print(f"!!! WARNING: Dataset error ({e}). Using synthetic.")

    def _resolve_tokenizer(self, model_id: str):
        """
        Return the tokenizer associated with the requested model, falling back
        to the shared GPT-2 tokenizer if needed.
        """
        return self.tokenizers.get(model_id) or self.fallback_tokenizer

    def _tensor_parallel_size(self, model_id: str) -> int:
        """
        Retrieve the configured tensor_parallel_size for the requested model.
        """
        spec = self.model_specs.get(model_id, {})
        tp = spec.get('tensor_parallel_size', 1)
        try:
            tp_value = int(tp)
        except (TypeError, ValueError):
            tp_value = 1
        return max(tp_value, 1)

    def _get_exact_content(self, target_len: int, hard_limit: int, model_id: str) -> tuple[str, int]:
        """
        Returns text guaranteed to be <= hard_limit tokens FOR THE SPECIFIC MODEL
        and the number of tokens generated.
        """
        # 1. Select Content
        if self.prompt_bank:
            text = random.choice(self.prompt_bank)
        else:
            # Synthetic Fallback
            vocab = ["scientific", "distributed", "system", "latency", "gpu", "kernel", "queue"]
            text = " ".join(random.choices(vocab, k=max(1, int(target_len))))

        # 2. Select Correct Tokenizer
        tokenizer = self._resolve_tokenizer(model_id)
        if not tokenizer:
            # Absolute worst case: use whitespace approximation
            truncated = text[:hard_limit*4]
            return truncated, len(truncated.split())

        # 3. Tokenize & Truncate
        tokens = tokenizer.encode(text)
        
        # If too long, chop it
        if len(tokens) > hard_limit:
            tokens = tokens[:hard_limit]
            text = tokenizer.decode(tokens, skip_special_tokens=True)
            
        # If too short (and synthetic), grow it
        elif len(tokens) < target_len and not self.prompt_bank:
            while len(tokens) < target_len:
                tokens += tokens
            tokens = tokens[:target_len]
            text = tokenizer.decode(tokens, skip_special_tokens=True)

        return text, len(tokens)

    def generate_trace(self, exp_config: ExpConfig):
        trace_cfg = exp_config.job_trace_config
        if not isinstance(trace_cfg, TraceGeneratorConfig):
            print("!!! ERROR: trace_config must be TraceGeneratorConfig for generate_trace()")
            sys.exit(1)

        self._load_prompt_dataset(trace_cfg.input_prompt_path)

        source_path = trace_cfg.input_trace_path
        if not source_path:
            print("!!! ERROR: 'input_trace_path' missing in TraceGeneratorConfig.")
            sys.exit(1)

        print(f">>> [GEN] Loading Azure trace: {source_path}")
        df = pd.read_csv(source_path)
        
        # 1. Normalize Columns
        df.columns = [c.strip().lower() for c in df.columns]
        col_map = {c: c for c in df.columns}
        
        # Azure specific columns
        ts_col = next((c for c in df.columns if 'timestamp' in c), None)
        in_col = next((c for c in df.columns if 'context' in c), None)   # ContextTokens
        out_col = next((c for c in df.columns if 'generated' in c), None) # GeneratedTokens

        if not ts_col:
            print(f"!!! ERROR: Could not find TIMESTAMP column in {df.columns}")
            sys.exit(1)

        # 2. Sort & Parse Time
        df = df.sort_values(by=ts_col)
        if df[ts_col].dtype == object:
             df[ts_col] = pd.to_datetime(df[ts_col]).astype(int) / 10**9
        
        # 3. Window Sampling (Peak/Sparse/Random)
        target_duration = trace_cfg.duration
        strategy = trace_cfg.sampling_strategy
        
        start_times = df[ts_col].values
        total_time = start_times[-1] - start_times[0]
        
        if total_time <= target_duration:
            print(f"!!! WARNING: Trace duration ({total_time:.1f}s) < target ({target_duration}s). Using full trace.")
            selected_df = df.copy()
            best_window_start = start_times[0]
        else:
            print(f">>> [GEN] Strategy: {strategy.upper()} | Window: {target_duration}s")
            
            # Create 10s bins for histogram analysis
            bins = np.arange(start_times[0], start_times[-1], 10)
            counts, _ = np.histogram(start_times, bins)
            
            # Sliding window sum
            window_steps = max(1, int(target_duration / 10))
            rolling_counts = np.convolve(counts, np.ones(window_steps), mode='valid')
            
            if strategy == 'peak':
                best_idx = np.argmax(rolling_counts)
                best_window_start = bins[best_idx]
            elif strategy == 'sparse':
                valid_mask = rolling_counts > 0
                if valid_mask.any():
                    best_idx = np.argmin(rolling_counts[valid_mask])
                    real_indices = np.where(valid_mask)[0]
                    best_window_start = bins[real_indices[best_idx]]
                else:
                    best_window_start = bins[0]
            else: # Random
                max_start = start_times[-1] - target_duration
                best_window_start = random.uniform(start_times[0], max_start)

            print(f">>> [GEN] Selected Window Start: {best_window_start:.2f}")
            t_end = best_window_start + target_duration
            selected_df = df[(df[ts_col] >= best_window_start) & (df[ts_col] < t_end)].copy()

        # 4. Normalize Time
        selected_df['rel_timestamp'] = selected_df[ts_col] - best_window_start
        speedup = trace_cfg.speedup
        selected_df['rel_timestamp'] /= speedup
        
        # 5. Model Assignment (Zipfian)
        # Create weights based on model size (inverse)
        our_models = [list(m.keys())[0] for m in self.models_cfg]
        sizes = [list(m.values())[0].get('size', 10) for m in self.models_cfg]
        
        inv_sizes = [1.0 / max(s, 1) for s in sizes]
        total_w = sum(inv_sizes)
        weights = [x / total_w for x in inv_sizes]
        
        # Pre-assign models to rows
        # We use random.choices with weights for the whole column
        assigned_models = random.choices(our_models, weights=weights, k=len(selected_df))
        selected_df['target_model'] = assigned_models
        
        # ---------------------------------------------------------
        # THE MULTI-TOKENIZER LOOP
        # ---------------------------------------------------------
        mode_keys = list(trace_cfg.modes.keys())
        mode_weights = [trace_cfg.modes[k] for k in mode_keys]
        if sum(mode_weights) <= 0:
            mode_keys, mode_weights = ["chat"], [1]
        print(f">>> [GEN] Mode distribution: {dict(zip(mode_keys, mode_weights))}")

        output_rows = []
        for idx, row in selected_df.iterrows():
            target_model = row['target_model']
            max_model_len = self.model_specs[target_model]['max_model_len']
            req_out = int(row[out_col]) if out_col else 100
            req_out = max(1, req_out)
            req_in = int(row[in_col]) if in_col else 100
            available_input = max_model_len - req_out - 10
            if available_input < 1:
                continue
            final_input_len = min(req_in, available_input)

            prompt_text, input_len = self._get_exact_content(
                target_len=final_input_len,
                hard_limit=final_input_len,
                model_id=target_model
            )
            mode = random.choices(mode_keys, weights=mode_weights, k=1)[0]
            output_rows.append({
                "timestamp": float(f"{row['rel_timestamp']:.4f}"),
                "model": target_model,
                "mode": mode,
                "prompt": prompt_text,
                "input_len": input_len,
                "tensor_parallel_size": self._tensor_parallel_size(target_model),
                "output_len": req_out
            })

        # Save
        out_path = trace_cfg.output_trace_path
        
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, 'w', encoding='utf-8') as f:
            # Write Metadata Header
            metadata = {
                "__type__": "metadata",
                "generator_config": asdict(trace_cfg),
                "timestamp": pd.Timestamp.now().isoformat()
            }
            f.write(json.dumps(metadata) + "\n")

            for entry in output_rows:
                f.write(json.dumps(entry) + "\n")
        
        print(f">>> [GEN] SUCCESS. Saved {len(output_rows)} requests using multi-model tokenization to {out_path}.")
        
        config_path = exp_config.job_replay_client_config.config_path
        exp_config.save_yaml(config_path)
        print(f">>> [GEN] Saved config to {config_path}")
  
    def generate_weak_scaling(self, exp_config: ExpConfig):
        """
        Generates a synthetic, deterministic weak-scaling workload.
        Goal: Constant Rate, Constant Compute, Linear Volume.
        """
        ws_cfg = exp_config.job_trace_config
        if not isinstance(ws_cfg, WeakScalingConfig):
            print("!!! ERROR: trace_config must be WeakScalingConfig for generate_weak_scaling()")
            sys.exit(1)
        num_nodes = exp_config.model_deployment_config.num_nodes
        rate_per_node = ws_cfg.rpn
        duration = ws_cfg.duration
        
        self._load_prompt_dataset(ws_cfg.input_prompt_path)
        
        total_qps = num_nodes * rate_per_node
        total_requests = int(total_qps * duration)
        inter_arrival_time = 1.0 / total_qps if total_qps > 0 else 0

        # Fixed Compute Dimensions
        target_in = ws_cfg.input_len
        target_out = ws_cfg.output_len
        
        print(f">>> [WEAK SCALING] Nodes: {num_nodes} | Rate/Node: {rate_per_node} Hz")
        print(f"    Total QPS: {total_qps:.2f} | Total Reqs: {total_requests}")
        print(f"    Compute: {target_in} in / {target_out} out (Fixed)")

        # 2. Prepare Resources
        # We need to cycle through models evenly (Round Robin)
        # Assuming all models are deployed on all nodes for this baseline
        model_names = [list(m.keys())[0] for m in self.models_cfg]
        if not model_names:
            print("!!! ERROR: No models defined in config.")
            sys.exit(1)

        # 3. Generate Trace
        output_rows = []
        current_time = 0.0
        
        # Tokenizer for precision
        # We use the first model's tokenizer as the standard ruler
        first_model = model_names[0]
        
        print(">>> [GEN] Generating payload...")
        for i in range(total_requests):
            # A. Deterministic Timing (Perfect spacing)
            current_time += inter_arrival_time
            
            # B. Deterministic Routing (Round Robin)
            model = model_names[i % len(model_names)]
            
            # C. Deterministic Content (Unique but Fixed Length)
            # We generate unique text to bypass cache, but force exact length
            # Seed the random generator with the index to ensure reproducibility
            # but uniqueness per request.
            random.seed(self.seed + i) 
            
            # We use the generic 'get_exact_content' we wrote before, 
            # but we force it to generate fresh text every time.
            prompt_text, input_len = self._get_exact_content(
                target_len=target_in, 
                hard_limit=target_in, 
                model_id=model
            )
            
            output_rows.append({
                "timestamp": float(f"{current_time:.6f}"),
                "model": model,
                "prompt": prompt_text,
                "input_len": input_len,
                "tensor_parallel_size": self._tensor_parallel_size(model),
                "output_len": target_out
            })

            if i % 1000 == 0:
                print(f"\r    Generated {i}/{total_requests}", end="")

        # 4. Save
        out_path = ws_cfg.output_trace_path
        
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, 'w', encoding='utf-8') as f:
            # Write Metadata Header
            metadata = {
                "__type__": "metadata",
                "generator_config": asdict(ws_cfg),
                "timestamp": pd.Timestamp.now().isoformat()
            }
            f.write(json.dumps(metadata) + "\n")

            for entry in output_rows:
                f.write(json.dumps(entry) + "\n")
                
        print(f"\n>>> [GEN] SUCCESS. Saved to {out_path}")
        
        config_path = exp_config.job_replay_client_config.config_path
        exp_config.save_yaml(config_path)
        print(f">>> [GEN] Saved config to {config_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp-type", choices=["peak", "burst", "sparse"], default="sparse", 
                       help="Type of experiment config to use")
    args = parser.parse_args()
    
    # Get ExpConfig
    if args.exp_type == "peak":
        exp_cfg = peak_trace_config()
    elif args.exp_type == "burst":
        exp_cfg = burst_trace_config()
    elif args.exp_type == "sparse":
        exp_cfg = sparse_trace_config()
    else:
        print(f"!!! ERROR: Invalid experiment type '{args.exp_type}'. Must be one of: peak, burst, sparse")
        sys.exit(1)
    
    print(f">>> [MAIN] Running {args.exp_type} trace generation...")
    
    # Initialize generator with ExpConfig
    gen = TraceGenerator(exp_cfg)
    
    # Generate trace
    gen.generate_trace(exp_cfg)
    
    print(f">>> [MAIN] Complete. Trace saved to {exp_cfg.job_trace_config.output_trace_path}")
