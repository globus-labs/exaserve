## Optimizations

### [03/04/2026] Client side unnecessary environment setup cleanup
- The client now uses `litellm` venv which needs to specifically set in the environment - currently using python to inject, we can simply move the setup before the command in the bash scripts.
- The prints are all dumped in the same stdout/stderr. Need to categorize them into different stream and save them for further analysis, or we have our own log dedup strategy globally, which only prints necessary error output and collapse similar INFO output with singleline by several "x"
- A static port assignment may be a potential issue if the port is gone. A robust way is we capture the port and capture the code.
- experiments folder should be moved to somewhere else, pbs_output and results should be in the same folder so we don't need to do manul indexing every time we check the results.


## Experiments Ideas
- Raw latency including the LiteLLM proxy hop
- Proxy overhead measured in isolation
- Error rate before vs. after retries (?)
