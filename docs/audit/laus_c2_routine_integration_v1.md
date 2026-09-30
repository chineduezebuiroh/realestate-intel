# LAUS C2 routine integration decision

LAUS routine production uses the governed cycle target month as its deterministic
annual reference year and the existing vintage identity
`bls-laus-annual-processing-v1:<year>`. Before July, the routine uses the exact
three-inclusive-year ordinary overlap. Beginning with the first eligible July
cycle, an unsatisfied vintage selects full history (1976 through the explicit
execution end year). A successful satisfaction for that exact vintage returns
later cycles to ordinary overlap.

July 1 is a conservative **not-before operational guardrail**. It is not a claim
that BLS annual processing completes on a fixed date. C1/C1b publication,
January-release, RSS, and processing-class evidence remains governed diagnostic
provider evidence, but is not a prerequisite or selection authority for C2.
Numeric changes, footnotes, and operator assertions are likewise not authority.

The satisfaction write occurs only after canonical validation, immutable
publication verification, construction of a successful common source execution
result, and durable recording of that cycle result. Failed acquisition,
validation, publication, or result recording leaves the vintage unsatisfied and
eligible for normal/resume retry. Ordinary overlap cannot satisfy a vintage.
The durable LAUS cycle-result record binds the selected and executed acquisition
mode to the exact cycle and annual vintage. If a job stops after recording an
annual-deep result but before satisfaction, resume repairs satisfaction from that
durable proof without acquisition or publication. Ordinary results cannot repair it.
Replay skips both cycle-result recording and satisfaction, and source publication
never advances the accepted LAUS pointer.

External monitoring remains a human exception-investigation backstop only. It is
not production control-plane input. No independent LAUS schedule exists; the
master monthly cohort invokes LAUS in parallel with FRED and CES and includes it
in the common `needs`/`always()` barrier.
