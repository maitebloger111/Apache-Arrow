# Bounty Fix

**Reward:** $10.00
**Issue:** https://github.com/maitebloger111/Apache-Arrow/issues/1
**Repo:** https://github.com/maitebloger111/Apache-Arrow

---

## Issue

🎯 Optimize Partition Pruning and Expression Evaluation for Large Partitioned Parquet Datasets

## 📝 Description
When querying large Parquet datasets partitioned across thousands of directories (e.g., Hive-style partitioning with high cardinality keys), the dataset filtering and query planning phases exhibit significant latency. Currently, the dataset scanner evaluates filter expressions against partition keys in a manner that scales poorly with the number of fragments. 

The overhead is primarily driven by:
1. Redundant or eager metadata loading of Parquet file footers before partition pruning has fully eliminated irrelevant fragments.
2. Inefficient expression simplification and evaluation against partition expressions within the `arrow::dataset::Dataset` and `arrow::dataset::Fragment` APIs.
3. Synchronous, single-threaded partition pruning during fragment discovery.

This issue aims to optimize the partition pruning pipeline to ensure that irrelevant fragments are discarded as early as possible in the planning phase, minimizing I/O and CPU overhead during dataset scans.

## 🎯 Acceptance Criteria
- [ ] Partition pruning must occur *before* resolving or inspecting individual Parquet file schemas/metadata (footers) to prevent unnecessary I/O.
- [ ] Optimize the expression evaluation engine (`arrow::dataset::Expression`) when matching filters against partition guarantees, reducing CPU overhead during fragment filtering.
- [ ] Implement parallelized partition pruning/fragment filtering where applicable, or optimize the sequential execution path to handle $>100,000$ fragments in under 100ms.
- [ ] Ensure dictionary-encoded partition keys do not trigger premature decoding/unpacking during expression evaluation.
- [ ] No regressions in existing dataset filtering, schema evolution, or projection tests.

## 🛠️ Technical Specifications & Context
The core of the dataset filtering logic resides in the C++ implementation of the Arrow Dataset module:
* **Fragment Filtering & Discovery**: `cpp/src/arrow/dataset/discovery.cc` and `cpp/src/arrow/dataset/dataset.cc`. The `Dataset::GetFragments` method evaluates the filter expression against the partition expression of each fragment.
* **Parquet Fragment Handling**: `cpp/src/arrow/dataset/file_parquet.cc`. Ensure `ParquetFileFragment::Inspect` or metadata loading is deferred until after the fragment has cleared the partition filter.
* **Expression Simplification**: `cpp/src/arrow/dataset/expression.cc`. The `SimplifyWithGuarantee` function should be profiled to identify bottlenecks when evaluating complex filter expressions against partition schemas.

### Suggested Implementation Steps:
1. **Defer Metadata Loading**: Modify `ParquetDatasetFactory` and `FileSystemDataset` to ensure that fragment filtering using partition expressions is executed strictly prior to any file-level metadata inspection.
2. **Batch Expression Evaluation**: Instead of evaluating the filter expression against each fragment's partition expression individually, batch the evaluation or pre-filter fragments using a multi-threaded execution context (e.g., using the Arrow `Executor`).
3. **Optimize Expression Simplification**: Cache simplified expressions where partition keys are identical across multiple fragments to avoid redundant AST traversals.

## 🧪 Verification & Testing
Hunter can verify the fix using the following methods:

### 1. C++ Unit Tests
Run the dataset and Parquet-specific tests to ensure functional correctness:
```bash
cmake --build . --target arrow-dataset-test arrow-dataset-file-parquet-test
./release/arrow-dataset-test
./release/arrow-dataset-file-parquet-test
```

### 2. Python Benchmark / Reproduction Script
Verify the performance improvement using `pyarrow.dataset`. The planning time for the following script should decrease significantly:

```python
import time
import tempfile
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Create a mock partitioned dataset with 10,000 partitions
with tempfile.TemporaryDirectory() as tmpdir:
    table = pa.table({"a": [1], "b": [2], "data": ["test"]})
    
    # Write 10,000 small partitioned files
    for i in range(10000):
        partition_path = f"{tmpdir}/a={i}/b=2"
        pq.write_table(table.select(["data"]), partition_path + "/data.parquet")
    
    # Load dataset
    dataset = ds.dataset(tmpdir, format="parquet", partitioning="hive")
    
    # Measure query planning + filtering time
    start_time = time.perf_counter()
    
    # Filter for a single partition
    scanner = dataset.scanner(filter=(ds.field("a") == 5000) & (ds.field("b") == 2))
    result_table = scanner.to_table()
    
    end_time = time.perf_counter()
    print(f"Time taken to filter and load: {end_time - start_time:.4f} seconds")
    assert len(result_table) == 1
```

---

[![Opire Bounty](https://img.shields.io/badge/bounty-Opire-emerald?style=for-the-badge)](https://github.com/maitebloger111/Apache-Arrow)
<br/>
<hr/>

<details><summary>This repo is using Opire - what does it mean? 👇</summary><br/>💵 Everyone can add rewards for this issue commenting <code>/reward 100</code> (replace <code>100</code> with the amount).<br/>🕵️‍♂️ If someone starts working on this issue to earn the rewards, they can comment <code>/try</code> to let everyone know!<br/>🙌 And when they open the PR, they can comment <code>/claim #1</code> either in the PR description or in a PR's comment.<br/><br/>🪙 Also, everyone can tip any user commenting <code>/tip 20 @maitebloger111</code> (replace <code>20</code> with the amount, and <code>@maitebloger111</code> with the user to tip).<br/><br/>📖 If you want to learn more, check out our <a href="https://docs.opire.dev">documentation</a>.</details>

---

## Process

### 1. Orient yourself first
Before touching any code:
- Read the README and any relevant docs
- Run: find . -type f | grep -v node_modules | grep -v .git | grep -v dist | head -60
- Grep for the key terms from the issue title to find the relevant files
- Read the files around the bug — understand what's currently happening

### 2. Find and read the tests
- Look for test files near the relevant code (*.test.ts, *.spec.js, __tests__, etc.)
- Read them — they tell you what behavior is expected
- Understand what a passing fix looks like before writing a line

### 3. Implement the fix
- Make the MINIMAL change that fixes the issue — nothing more
- Do not refactor, rename, reorganize, or clean up unrelated code
- Do not add dependencies unless the issue explicitly requires it
- Do not modify config files (package.json, tsconfig.json, etc.) unless that IS the fix
- One logical change, one commit

### 4. Verify
- Run the test suite: check package.json scripts for "test", "lint", "typecheck"
  Common commands: npm test, yarn test, pnpm test, pytest, go test ./..., cargo test
- If TypeScript is present: npx tsc --noEmit
- Fix any test failures your change caused — if a test was wrong before, update it

### 5. Commit and document
- git add -A
- git commit -m "fix: 🎯 Optimize Partition Pruning and Expression Evaluation for Large Partit"
- Write AGENT_SUMMARY.md with:
  - Root cause: what was wrong and why
  - Fix: what you changed and the reasoning
  - Verification: how to confirm it works
  - Concerns: anything a reviewer should know

---

## Stop conditions

If ANY of these are true, write "UNCERTAIN: [reason]" at the top of AGENT_SUMMARY.md, make NO code changes, and stop:
- The fix requires changing more than ~5 files
- You cannot determine what the correct behavior should be
- The issue requires a database migration, API key, or external service
- You find 3+ open PRs already attempting this fix (high competition)
- The repo is not in a language/framework you can reason about confidently

A clean UNCERTAIN is better than a bad PR.
