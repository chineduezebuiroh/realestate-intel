# Macro Regime static deployment contract

## Status and architecture

This contract begins visualization **Phase 2**: deterministic static build,
governed artifact handoff, and GitHub Pages publication. The renderer remains an
analytically read-only consumer of an explicit immutable production run. The
pipeline never discovers `latest`, refreshes data, reruns the regime engine, or
uses DuckDB/Python in the published browser experience.

Generated deployment files live in `site/`, separate from both immutable input
runs and ignored analytical products. `site/` is a transient GitHub Actions
artifact and is not committed. GitHub Pages is preferred because its official
artifact deployment supports repository-path URLs without a generated-content
branch.

## Deterministic build

The authoritative Washington, DC command is:

```bash
PYTHONPATH=. python -u scripts/build_macro_regime_site.py \
  --run artifacts/regime/runs/capital_markets_f4_production_20260818 \
  --output site
```

The run is mandatory and explicit. The builder fails if it is absent or if the
destination is inside the run, clears the destination to prevent stale county
publication, and publishes only `district_of_columbia_dc__county`. It does not
alter evaluation date, geography, run identity, visualization version, schema,
registry hashes, scores, regimes, or narrative semantics.

Output is:

```text
site/
  index.html
  manifest.json
  counties/
    district_of_columbia_dc__county.html
    district_of_columbia_dc__county_snapshot.json
```

The landing page lists available markets with regime, Demand, Supply, and as-of
date. Its Washington, DC link is
`counties/district_of_columbia_dc__county.html`; that direct URL is refresh-safe.
The county page links back with `../index.html`. These relative paths work at a
GitHub Pages project URL such as `https://<owner>.github.io/realestate-intel/`
and preserve the county-first architecture for later explicit expansion.

## Governed artifact handoff and deployment

Production runs are intentionally ignored and absent from clean checkouts. The
workflow therefore **fails closed** until maintainers create a governed GitHub
Release publish bundle from the authoritative immutable run. It never uses a
fixture, substitutes another run, or creates production evidence.

The release asset contract is a gzip tar archive whose single top-level
directory is exactly `capital_markets_f4_production_20260818/` and contains the
authoritative run files unchanged. Create it locally from the directory that
contains the run:

```bash
tar -czf capital_markets_f4_production_20260818.tar.gz \
  capital_markets_f4_production_20260818
sha256sum capital_markets_f4_production_20260818.tar.gz
```

Upload that archive to a protected GitHub Release, record its SHA-256, and set
these repository Actions variables:

* `MACRO_REGIME_ARTIFACT_RELEASE_TAG` — immutable release tag;
* `MACRO_REGIME_ARTIFACT_ASSET` — exact archive filename;
* `MACRO_REGIME_ARTIFACT_SHA256` — reviewed archive digest.

The manually triggered `Deploy Macro Regime site` workflow downloads the named
release asset with the repository token, verifies the digest before extraction,
requires the exact run ID, installs pinned repository dependencies, builds and
smoke-tests the site, uploads a Pages artifact, and deploys it. A missing asset,
variable, digest match, directory, required parquet, or governed registry stops
publication. The release is the immutable handoff; the Pages artifact is only a
derived delivery package.

Required GitHub configuration is: Pages **Source = GitHub Actions**, Actions
enabled, read access to the governed release, and deployment approval/protection
on the `github-pages` environment if the repository requires it. The workflow's
least-privilege permissions grant read-only contents plus Pages and OIDC deploy
rights. Publication is manual by design; run it with **Actions → Deploy Macro
Regime site → Run workflow** after the governed release and variables exist.

## Manifest and browser packaging

`manifest.json` exposes snapshot schema and visualization versions, production
run ID, generated county identity/name, registry filenames and SHA-256 identities,
and the path, size, and SHA-256 for every published content file. The county
snapshot additionally exposes geography, evaluation date, current persisted
state, cadence freshness, and provenance identities. Checkout-local absolute
paths are deliberately excluded while immutable IDs and hashes are preserved.

Plotly remains embedded in each county HTML file, so interaction does not depend
on a CDN and works under GitHub Pages HTTPS. Published files require no Python,
DuckDB, API, server state, or checkout location. JavaScript-capable browsers—not
mail or mobile attachment previewers—are the supported delivery surface.

## Smoke and acceptance

Deployment Smoke 108 accepts only an explicit run and builds twice into fresh
temporary directories. It verifies the index, county HTML, snapshot, exact
determinism, manifest identities/hashes/sizes, county-only inventory, production
identity, schema/version, Plotly inclusion, relative index/county navigation,
and absence of `file://` and common absolute filesystem roots. It is separate
from visualization Smoke 107 and runs before every Pages upload:

```bash
PYTHONPATH=. python -u scripts/smoke_tests/100_109/108_macro_regime_static_deployment.py \
  --run artifacts/regime/runs/capital_markets_f4_production_20260818
```

After deployment, acceptance uses the Pages root and direct county URL.

### Desktop

* root and direct county pages load with no broken links;
* Plotly renders, responds to hover, and remains interactive;
* sticky navigation and back-to-index navigation work;
* native evidence/audit disclosures open and close;
* refresh preserves both routes.

### Mobile Safari and Chrome

* root and direct county pages load and refresh;
* responsive Plotly charts render without page-level horizontal overflow;
* sticky navigation can be horizontally scrolled and used;
* disclosure controls remain usable and county navigation works.

The landing table has its own narrow-screen scroll container; it does not widen
the page. Attachment preview support is explicitly not acceptance criteria.

## Local preview

Build first, then serve the output over HTTP:

```bash
cd site
python -m http.server 8000 --bind 0.0.0.0
```

Open `http://localhost:8000/` or the direct county path in a normal browser.
Mobile attachment viewers may block JavaScript and are not supported.

## Failure modes and future extension

Missing authoritative artifacts are an infrastructure boundary, not permission
to fabricate a site. A checksum mismatch indicates an unreviewed or corrupted
handoff. Missing parquet inputs, contradictory registry membership, non-county
geography, or provenance reconciliation errors fail in the existing renderer.
Pages environment rejection and absent release permissions fail before deploy.

Future refresh orchestration can publish a new immutable release asset, update
the explicit run ID/digest under review, and invoke the same build. Future county
expansion should replace the builder's one-county publication list with an
explicit governed county manifest. CBSA, mutable databases, APIs, authentication,
forecasting, and static chart fallbacks remain out of scope.
