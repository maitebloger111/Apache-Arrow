@@ -8,6 +8,8 @@
 #include "arrow/filesystem/filesystem.h"
 #include "arrow/util/iterator.h"
 #include "arrow/util/logging.h"
+#include "arrow/util/parallel.h"
+#include "arrow/util/thread_pool.h"
 
 #include <atomic>
 #include <mutex>
@@ -107,46 +109,70 @@
 Result<FragmentVector> FileSystemDataset::GetFragments(
     const FragmentFilter& fragment_filter) {
   // If no fragments, return empty vector
-  if (fragment_uris_.empty()) {
+  if (fragment_uris_.empty() || partition_expressions_.empty()) {
     return FragmentVector{};
   }
 
   const auto& scan_filter_expr = fragment_filter.filter_expression();
 
   // If the scan filter is trivial true, no pruning needed.
-  // Return all fragments without further processing.
+  // In this case, we just need to materialize all fragments.
   if (scan_filter_expr.IsTrivialTrue()) {
     FragmentVector all_fragments;
     all_fragments.reserve(fragment_uris_.size());
     for (size_t i = 0; i < fragment_uris_.size(); ++i) {
-      ARROW_ASSIGN_OR_RAISE(auto fragment, GetFragment(fragment_uris_[i], partition_expressions_[i]));
+      ARROW_ASSIGN_OR_RAISE(
+          auto fragment, GetFragment(fragment_uris_[i], partition_expressions_[i]));
       all_fragments.push_back(std::move(fragment));
     }
     return all_fragments;
   }
 
-  FragmentVector fragments;
-  fragments.reserve(fragment_uris_.size());
-
-  for (size_t i = 0; i < fragment_uris_.size(); ++i) {
-    ARROW_ASSIGN_OR_RAISE(auto fragment,
-                          GetFragment(fragment_uris_[i], partition_expressions_[i]));
-
-    // If the fragment has no partition expression (e.g., not partitioned),
-    // we cannot prune it based on partition keys. It must be kept.
-    if (fragment->partition_expression().IsTrivialTrue()) {
-      fragments.push_back(std::move(fragment));
-      continue;
-    }
-
-    // Use Expression::Assume to simplify the scan filter given the fragment's partition.
-    // If the simplified filter becomes 'false', then the fragment is pruned.
-    ARROW_ASSIGN_OR_RAISE(
-        auto simplified_filter,
-        scan_filter_expr.Assume(fragment->partition_expression()));
-
-    if (!simplified_filter.IsTrivialFalse()) {
-      fragments.push_back(std::move(fragment));
+  // Use a thread pool for parallel processing of fragments.
+  // GetCpuThreadPool() provides a global shared thread pool.
+  auto pool = arrow::internal::GetCpuThreadPool();
+
+  // Use a mutex to protect access to the collected_fragments vector.
+  // This ensures thread-safe addition of fragments that pass the filter.
+  std::mutex collected_fragments_mutex;
+  FragmentVector collected_fragments;
+  collected_fragments.reserve(fragment_uris_.size()); // Pre-reserve capacity
+
+  // Use an atomic status to propagate errors from parallel tasks.
+  std::atomic<Status> parallel_status = Status::OK();
+
+  // Parallelize the loop over fragment URIs.
+  // The lambda function processes a range of indices [start, end).
+  RETURN_NOT_OK(pool->ParallelFor(0, fragment_uris_.size(), [&](int i) -> Status {
+    // If an error has already occurred in another thread, stop processing.
+    if (!parallel_status.load().ok()) {
+      return parallel_status.load();
+    }
+
+    // Create the fragment. GetFragment is typically cheap and doesn't read file footers.
+    Result<std::shared_ptr<Fragment>> fragment_result =
+        GetFragment(fragment_uris_[i], partition_expressions_[i]);
+
+    if (!fragment_result.ok()) {
+      parallel_status.store(fragment_result.status());
+      return fragment_result.status();
+    }
+    std::shared_ptr<Fragment> fragment = *fragment_result;
+
+    const auto& fragment_partition_expr = fragment->partition_expression();
+
+    // If the fragment has no partition expression (e.g., not partitioned),
+    // we cannot prune it based on partition keys. It must be kept.
+    if (fragment_partition_expr.IsTrivialTrue()) {
+      std::lock_guard<std::mutex> lock(collected_fragments_mutex);
+      collected_fragments.push_back(std::move(fragment));
+      return Status::OK();
+    }
+
+    // Use Expression::Assume to simplify the scan filter given the fragment's partition.
+    // If the simplified filter becomes 'false', then the fragment is pruned.
+    Result<Expression> simplified_filter_result =
+        scan_filter_expr.Assume(fragment_partition_expr);
+
+    if (!simplified_filter_result.ok()) {
+      parallel_status.store(simplified_filter_result.status());
+      return simplified_filter_result.status();
+    }
+    const Expression& simplified_filter = *simplified_filter_result;
+
+    if (!simplified_filter.IsTrivialFalse()) {
+      // The filter is still satisfiable or simplified to something else. Keep the fragment.
+      std::lock_guard<std::mutex> lock(collected_fragments_mutex);
+      collected_fragments.push_back(std::move(fragment));
     }
-  }
-  return fragments;
+    return Status::OK();
+  }));
+
+  // Check if any error occurred during parallel execution.
+  RETURN_NOT_OK(parallel_status.load());
+
+  return collected_fragments;
 }
 
 Result<std::shared_ptr<Schema>> FileSystemDataset::ReadSchema(
```