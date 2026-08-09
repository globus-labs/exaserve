# Retired ad-hoc runners

ClientLab studies run only through the versioned `python3 -m clientlab`
interface and the specs under `clientlab/specs/`. The former one-off shell
runners were removed during the production cutover because they constructed a
second process lifecycle, accepted unbound raw endpoints, embedded user/site
paths, and could report results without canonical plan or readiness evidence.

Use:

```bash
python3 -m clientlab plan client_microbench
python3 -m clientlab smoke client_microbench
python3 -m clientlab run path/to/study.yaml --local
```

For `target.type: exaserve`, the study must reference an immutable RunPlan,
trace, generation, and deployment status directory. ClientLab then resolves
the endpoint only through the generation-bound deployment status API.
