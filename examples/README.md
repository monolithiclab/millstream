# Examples

The directories here are test fixtures and tools, not starting points:

| Directory | What it is |
| --- | --- |
| `m1-fixture`, `m2-fixture`, `m5-distill` | Packages the end-to-end suites run, on synthetic listings |
| `fakemodel` | A stand-in for an OpenAI-compatible model: the getting-started guide records its agent step against it, and the listings example's recordings come from it |

To try Millstream, scaffold the listings example, which runs offline with no build step and no model key:

```sh
millstream init pipeline listings --from listings
```

[The quickstart](../docs/guides/quickstart.md) runs it to a review in the web UI, and
[getting started](../docs/guides/getting-started.md) builds a pipeline from scratch.
