window.BENCHMARK_DATA = {
  "lastUpdate": 1788784466012,
  "repoUrl": "https://github.com/ioncode/idempotency",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "name": "ioncode",
            "username": "ioncode"
          },
          "committer": {
            "name": "ioncode",
            "username": "ioncode"
          },
          "id": "8681b6d08a1bb5168e6cd8b0855fb41816218ed4",
          "message": "tested rc for CI check",
          "timestamp": "2026-09-07T07:56:54Z",
          "url": "https://github.com/ioncode/idempotency/pull/1/commits/8681b6d08a1bb5168e6cd8b0855fb41816218ed4"
        },
        "date": 1788784465115,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMiddleware_Processing",
            "value": 4565,
            "unit": "ns/op\t    8054 B/op\t      35 allocs/op",
            "extra": "256042 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - ns/op",
            "value": 4565,
            "unit": "ns/op",
            "extra": "256042 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - B/op",
            "value": 8054,
            "unit": "B/op",
            "extra": "256042 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "256042 times\n4 procs"
          }
        ]
      }
    ]
  }
}