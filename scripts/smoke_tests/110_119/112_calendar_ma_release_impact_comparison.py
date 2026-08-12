"""Deterministic smoke for the persisted calendar-MA release comparator."""
from __future__ import annotations

from pathlib import Path
import tempfile
import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.experiments.calendar_ma_release_impact import (
    BASELINE_ID, CANDIDATE_ID, GEOS, build_review, circular_angle_difference,
    classify_rows,
)


def write_run(path: Path, candidate: bool = False, source_drift: bool = False) -> None:
    path.mkdir()
    dates = pd.date_range(
    "2025-08-31",
    "2026-07-31",
    freq=MONTH_END,
)
    base=pd.MultiIndex.from_product([GEOS,dates,["labor_force","employment","laus_unemployment_rate"]],names=["geo_id","date","canonical_metric_key"]).to_frame(index=False)
    base["value"]=100.0+np.arange(len(base))
    base.loc[base.date.eq("2025-10-31"),"value"]=np.nan
    if source_drift: base.loc[0,"value"]+=1
    base.to_parquet(path/"source_metrics.parquet")
    f=base.rename(columns={"canonical_metric_key":"metric_key"}); f["feature_key"]=f.metric_key+"_ma6_level"; f["raw_feature_value"]=f.value
    if candidate: f.loc[f.date.eq("2025-10-31"),"raw_feature_value"]=f.groupby(["geo_id","metric_key"]).raw_feature_value.transform(lambda x:x.ffill())
    f[["geo_id","date","metric_key","feature_key","raw_feature_value"]].to_parquet(path/"features.parquet")
    n = f[
        [
            "geo_id",
            "date",
            "metric_key",
            "feature_key",
        ]
    ].copy()
    n["feature_score"] = f.raw_feature_value / 100
    n.to_parquet(path/"normalized_features.parquet")
    m=f[["geo_id","date","metric_key"]].copy(); m["metric_score"]=f.raw_feature_value/100
    m.to_parquet(path/"metric_scores.parquet"); m.rename(columns={"metric_score":"aligned_metric_score"}).to_parquet(path/"aligned_metric_scores.parquet")
    d=base[["geo_id","date"]].drop_duplicates(); d["dimension"]="Demand"; d["dimension_score"]=.2+(candidate*.001)
    d.to_parquet(path/"dimension_scores.parquet")
    axes=[]
    for axis,value in (("Demand",.2+(candidate*.001)),("Supply",-.1)):
        q=d[["geo_id","date"]].copy(); q["axis"]=axis; q["axis_score"]=value; axes.append(q)
    pd.concat(axes).to_parquet(path/"axis_scores.parquet",index=False)
    co=d[["geo_id","date"]].copy(); co["x_supply"]=-.1; co["y_demand"]=.2+(candidate*.001); co["radius"]=np.hypot(co.x_supply,co.y_demand); co["angle_degrees"]=np.degrees(np.arctan2(co.y_demand,co.x_supply))%360
    co.to_parquet(path/"coordinates.parquet")
    r=d[["geo_id","date"]].copy(); r["major_regime"]="Expansion"; r["minor_regime"]="Broad"; r["quadrant"]="Q2"; r["regime_strength"]=.5; r["distance_to_boundary"]=.1
    if candidate: r.loc[(r.geo_id.eq(GEOS[0]))&r.date.eq("2025-10-31"),"minor_regime"]="Narrow"
    r.to_parquet(path/"regime_assignments.parquet")


with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp); b=root/BASELINE_ID; c=root/CANDIDATE_ID
    write_run(b); write_run(c,True)
    # Classification and delta math.
    left=pd.DataFrame({"k":[1,2,3],"value":[1.,2.,3.]}); right=pd.DataFrame({"k":[1,2,4],"value":[1.,2.5,4.]})
    classified=classify_rows(left,right,["k"])
    assert classified.set_index("k").classification.to_dict()=={1:"unchanged",2:"value_changed",3:"baseline_only",4:"candidate_only"}
    assert classified.loc[classified.k.eq(2),"absolute_delta"].iloc[0]==.5
    assert circular_angle_difference(pd.Series([359.,10.]),pd.Series([1.,350.])).tolist()==[2.,20.]
    # Exact identities and missing runs fail closed without output.
    try: build_review(root/"wrong",c,root/"bad")
    except ValueError: pass
    else: raise AssertionError("run identity must fail closed")
    # Source drift and geography leakage fail before output creation.
    candidate_source=pd.read_parquet(c/"source_metrics.parquet")
    drifted=candidate_source.copy(); drifted.loc[0,"value"]+=1; drifted.to_parquet(c/"source_metrics.parquet")
    try: build_review(b,c,root/"drift-output")
    except ValueError as exc: assert "source metrics differ" in str(exc)
    else: raise AssertionError("source drift must fail closed")
    assert not (root/"drift-output").exists()
    candidate_source.to_parquet(c/"source_metrics.parquet")
    # Full-universe run artifacts may contain additional geographies.
    # Extra non-governed geography rows must be ignored after scoping.
    extra = candidate_source.iloc[[0]].copy()
    extra["geo_id"] = "unexpected_county__county"

    with_extra = pd.concat(
        [candidate_source, extra],
        ignore_index=True,
    )

    with_extra.to_parquet(
        c / "source_metrics.parquet",
        index=False,
    )

    extra_out = root / "extra-geo-output"
    build_review(b, c, extra_out)

    if not extra_out.exists():
        raise AssertionError(
            "extra non-governed geography should be allowed and scoped out"
        )

    candidate_source.to_parquet(
        c / "source_metrics.parquet",
        index=False,
    )

    # Removing a governed geography must still fail closed.
    missing_governed = candidate_source.loc[
        ~candidate_source["geo_id"].eq(GEOS[-1])
    ].copy()

    missing_governed.to_parquet(
        c / "source_metrics.parquet",
        index=False,
    )

    try:
        build_review(
            b,
            c,
            root / "missing-governed-output",
        )
    except ValueError as exc:
        assert (
            "governed geography coverage missing"
            in str(exc)
        )
    else:
        raise AssertionError(
            "missing governed geography must fail closed"
        )

    candidate_source.to_parquet(
        c / "source_metrics.parquet",
        index=False,
    )
    # Complete end-to-end build and deterministic CSV contents.
    out1=root/"out1"; out2=root/"out2"; build_review(b,c,out1); build_review(b,c,out2)
    required={"calendar_ma_decision_matrix.csv","calendar_ma_governance_status.csv","calendar_ma_review.html","calendar_ma_change_attribution.csv"}
    assert required.issubset({p.name for p in out1.iterdir()})
    decision=pd.read_csv(out1/"calendar_ma_decision_matrix.csv"); governance=pd.read_csv(out1/"calendar_ma_governance_status.csv")
    assert decision.Candidate.tolist()==["REGIME-V1.0","REGIME-V1.0.1-CANDIDATE"] and decision.Decision.eq("pending").all()
    assert governance.human_decision.eq("pending").all() and not governance.automated_winner.astype(bool).any()
    for p in out1.glob("*.csv"):
        if p.name!="calendar_ma_runtime_summary.csv": assert p.read_bytes()==(out2/p.name).read_bytes()
    assert not any((root/x).exists() for x in ("production","config-write"))

print("Calendar MA release-impact comparison smoke test passed")
