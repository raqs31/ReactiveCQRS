---
project: ReactiveCQRS
assessed_at: 2026-09-16T00:00:00Z
agent_readiness: ready-with-compensation
context_type: brownfield
stack_components:
  language: Scala 2.13.18
  framework: Apache Pekko (actors) + ScalikeJDBC (event store/PostgreSQL access)
  build_tool: sbt 1.11.7
  test_runner: ScalaTest 3.2.19
  package_manager: sbt
  ci_provider: null
  deployment_target: null
gates_passed: 7
gates_failed: 1
---

## Stack Components

**Language**: Scala 2.13.18 (`project/Common.scala:26`, shared across all sub-projects via `Common.settings`).

**Framework**: Apache Pekko 1.4.0 (actors) for the event-sourcing engine, ScalikeJDBC 3.5.0 over PostgreSQL for the durable event/document store, and `mpjsons` 0.6.50 for JSON (de)serialization of events and documents (`CLAUDE.md:17-26`). This is itself a library/framework consumed by `neula-server`, not a runnable application — `testdomain` is a demo/test-only module (`CLAUDE.md:21`).

**Build tool**: sbt 1.11.7 (`project/build.properties`), multi-project build (`api`, `core`, `testutils`, `testdomain`, with `memory`/`postgres` currently commented out in `build.sbt`).

**Test runner**: ScalaTest 3.2.19 (`project/Common.scala`).

**CI/CD**: no CI config file found in the repo tree.

## Quality Gate Assessment

| Component  | Typed | Convention | Training Data | Documented | Verdict    |
|------------|-------|------------|----------------|------------|------------|
| Language   | ✓     | —          | —              | —          | pass       |
| Framework  | —     | ~          | ✗              | ✓          | fail       |
| Build tool | —     | ✓          | ✓              | ~          | pass       |
| Test runner| —     | —          | ✓              | ✓          | pass       |

Legend: ✓ = pass, ✗ = fail, ~ = partial, — = not applicable

### Gate Details

**Language — typed: pass.** Scala, statically typed. Evidence: `project/Common.scala:26`.

**Framework — convention: partial.** Neither Pekko nor ScalikeJDBC impose folder-layout/routing opinions — this is a library, so "convention" here means "is the module boundary and its API predictable." It is, but only because it's documented: `CLAUDE.md:31-40` gives a module table (`api`, `core`, `testutils`, `testdomain`) with purpose and dependency direction, and `CLAUDE.md` section 6 ("Conventions & gotchas for editing", line 207 onward) codifies editing rules.

**Framework — training data: fail.** Same root cause as `neula-server`: Apache Pekko is a 2022 Akka fork with a much thinner training-data footprint than Akka. Unlike `neula-server`, this repo already compensates for it explicitly — `CLAUDE.md:209` states: *"Pekko, not Akka. Imports are `org.apache.pekko.*`. Don't add akka deps."* This is exactly the ready-to-paste rule shape the compensation path calls for; it is called out here so it stays visible, not because it's missing.

**Framework — documented: pass.** Pekko and ScalikeJDBC both have current, versioned official docs.

**Build tool — sbt: pass with a documentation caveat.** sbt is the standard Scala build tool (training-data pass) and this project's module layout (`api/`, `core/`, `testutils/`, `testdomain/`) follows conventional sbt multi-project structure (convention pass). Official sbt docs exist and are versioned, but are commonly reported as harder to navigate than comparable tools (Maven, Gradle) — scored partial, not a fail, since the gap is thin and `CLAUDE.md:50-64` already documents this project's own sbt commands (compile/test per module, version-bump procedure), which is the correct compensation regardless.

**Test runner — ScalaTest: pass.** Dominant Scala test framework, well documented, common in training data.

## Gaps & Compensation

**Framework training-data gap (Pekko vs Akka).** Already compensated — `CLAUDE.md:209` carries the exact import-namespace warning an agent needs. No action required here; flagging it only so the pattern is visible as the template for `neula-server`'s equivalent gap (see that repo's assessment).

### Recommended Instruction File Additions

None required — the existing `CLAUDE.md:209` entry already covers the one real gap. No changes recommended.

## Summary

Overall verdict: **ready-with-compensation**, and the compensation is already in place. Scala's static typing, sbt's ubiquity in the Scala ecosystem, and ScalaTest's maturity give an agent solid footing; the module boundaries are clearly documented in `CLAUDE.md`. The sole structural gap — Pekko's training-data thinness relative to Akka — is already closed by an explicit rule in `CLAUDE.md:209`. This repo is the reference example for how `neula-server` should close its equivalent gap.

Next step: `/mario:health-check`.
