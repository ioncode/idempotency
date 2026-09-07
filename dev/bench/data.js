window.BENCHMARK_DATA = {
  "lastUpdate": 1788785257585,
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
      },
      {
        "commit": {
          "author": {
            "email": "ionrussia@gmail.com",
            "name": "Andrey Ponteleev",
            "username": "ioncode"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e715870f6702175283366bd3b486186912763ba1",
          "message": "ready for highload & network storms idempotency middleware \n\ntested rc for CI check",
          "timestamp": "2026-09-07T15:36:48+03:00",
          "tree_id": "2bd43065c07ce664baa829f5819a62a09e582c35",
          "url": "https://github.com/ioncode/idempotency/commit/e715870f6702175283366bd3b486186912763ba1"
        },
        "date": 1788785256920,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMiddleware_Processing",
            "value": 4300,
            "unit": "ns/op\t    8055 B/op\t      35 allocs/op",
            "extra": "232842 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - ns/op",
            "value": 4300,
            "unit": "ns/op",
            "extra": "232842 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - B/op",
            "value": 8055,
            "unit": "B/op",
            "extra": "232842 times\n4 procs"
          },
          {
            "name": "BenchmarkMiddleware_Processing - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "232842 times\n4 procs"
          }
        ]
      }
    ]
  }
}